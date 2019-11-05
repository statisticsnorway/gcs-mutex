package no.ssb.locking;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * This class implements mutual-exclusion through GCS object-metadata and preconditions. Although this class implements
 * the java.util.concurrent.Lock interface, this class will not ensure any happens-before thread-safety effects.
 * Due to limitations of if-generation-match:0 precondition, see https://cloud.google.com/storage/docs/generations-preconditions
 * it could be unsafe to delete the file in order to implement the unlock operation.
 * <p>
 * The GCSMetadataMutex is probably slightly faster than the GCSMutex because it never reads or writes object data,
 * just metadata. However, this metadata mutex requires object-admin access to the GCS bucket, whereas the data-mutex
 * only requires read-write access.
 */
public class GCSMetadataMutex implements Lock {

    private final static Logger LOG = LoggerFactory.getLogger(GCSMetadataMutex.class);

    public static GCSMetadataMutex create(Storage storage, BlobId mutexBlobId) {
        return new GCSMetadataMutex(storage, mutexBlobId, Duration.ofMinutes(1), new Random(), Duration.ofSeconds(64));
    }

    public static GCSMetadataMutex create(Storage storage, BlobId mutexBlobId, Duration timeToLive) {
        return new GCSMetadataMutex(storage, mutexBlobId, timeToLive, new Random(), Duration.ofSeconds(64));
    }

    public static GCSMetadataMutex create(Storage storage, BlobId mutexBlobId, Duration timeToLive, Random backoffRandom, Duration maximumBackoff) {
        return new GCSMetadataMutex(storage, mutexBlobId, timeToLive, backoffRandom, maximumBackoff);
    }

    public static GCSMetadataMutex create(Path serviceAccountKeyPath, String bucket, String path) throws GCSMutexException {
        return new GCSMetadataMutex(storageFrom(serviceAccountKeyPath), BlobId.of(bucket, path), Duration.ofMinutes(1), new Random(), Duration.ofSeconds(64));
    }

    public static GCSMetadataMutex create(Path serviceAccountKeyPath, String bucket, String path, Duration timeToLive) throws GCSMutexException {
        return new GCSMetadataMutex(storageFrom(serviceAccountKeyPath), BlobId.of(bucket, path), timeToLive, new Random(), Duration.ofSeconds(64));
    }

    public static GCSMetadataMutex create(Path serviceAccountKeyPath, String bucket, String path, Duration timeToLive, Random backoffRandom, Duration maximumBackoff) throws GCSMutexException {
        return new GCSMetadataMutex(storageFrom(serviceAccountKeyPath), BlobId.of(bucket, path), timeToLive, backoffRandom, maximumBackoff);
    }

    public static Storage storageFrom(Path serviceAccountKeyPath) throws GCSMutexException {
        ServiceAccountCredentials sourceCredentials;
        try {
            sourceCredentials = ServiceAccountCredentials.fromStream(Files.newInputStream(serviceAccountKeyPath, StandardOpenOption.READ));
        } catch (IOException e) {
            throw new GCSMutexException(e);
        }
        GoogleCredentials scopedCredentials = sourceCredentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/devstorage.full_control"));
        Storage storage = StorageOptions.newBuilder().setCredentials(scopedCredentials).build().getService();
        return storage;
    }

    final Storage storage;
    final BlobId mutexBlobId;
    final Duration timeToLive;
    final Duration maximumBackoff;
    final Random backoffRandom;

    public GCSMetadataMutex(Storage storage, BlobId mutexBlobId, Duration timeToLive, Random backoffRandom, Duration maximumBackoff) {
        this.storage = storage;
        this.mutexBlobId = mutexBlobId;
        this.timeToLive = timeToLive;
        this.maximumBackoff = maximumBackoff;
        this.backoffRandom = backoffRandom;
    }

    long computeBackoffWaitTimeMs(long i) {
        return Math.min(1000L * (1 << Math.min(i, 8)), maximumBackoff.toMillis()) + backoffRandom.nextInt(1001);
    }

    boolean tryAcquire() throws GCSMutexException {
        Blob blob = storage.get(mutexBlobId);
        if (blob == null) {
            return acquireByCreatingFileIfDoesNotExist();
        }
        Map<String, String> metadata = blob.getMetadata();
        String status = metadata.get("status");
        if (!"locked".equalsIgnoreCase(status)) {
            LOG.trace("Mutex available, attempting to acquire lock...");
            return acquireThroughMetadataUpdate(blob);
        }

        // already locked check whether lock has expired according to its defined ttl
        String existingTimeToLive = metadata.get("time-to-live");
        if (existingTimeToLive == null) {
            LOG.warn("Failed to acquire lock, existing lock has no expiry set: {}", metadata.get("uuid"));
            return false;
        }
        long ettl = Long.parseLong(existingTimeToLive);
        long lastModified = blob.getUpdateTime();
        long expiry = lastModified + ettl;
        long expiredDuration = System.currentTimeMillis() - expiry;
        if (expiredDuration >= 0) {
            LOG.trace("Mutex expired, attempting to acquire lock...");
            return acquireThroughMetadataUpdate(blob);
        }
        LOG.trace("Failed to acquire lock, already held by someone else with uuid: {} and ttl: {}", metadata.get("uuid"), metadata.get("time-to-live"));
        return false;
    }

    boolean acquireThroughMetadataUpdate(Blob blob) throws GCSMutexException {
        String uuid = UUID.randomUUID().toString();
        Map<String, String> m = Map.of(
                "uuid", uuid,
                "status", "locked",
                "time-to-live", String.valueOf(timeToLive.toMillis())
        );
        try {
            storage.update(blob.toBuilder().setMetadata(m).build(),
                    Storage.BlobTargetOption.generationMatch(),
                    Storage.BlobTargetOption.metagenerationMatch()
            );
            LOG.trace("Lock acquired. UUID: {}", uuid);
            return true;
        } catch (StorageException e) {
            if ("Precondition Failed".equals(e.getMessage())
                    && "conditionNotMet".equals(e.getReason())) {
                LOG.trace("Failed to acquire lock, lost race to another competing process");
                return false;
            } else {
                throw e; // unexpected
            }
        }
    }

    boolean acquireByCreatingFileIfDoesNotExist() throws GCSMutexException {
        // lock-file does not exist
        String uuid = UUID.randomUUID().toString();
        Map<String, String> metadata = Map.of(
                "uuid", uuid,
                "status", "locked",
                "time-to-live", String.valueOf(timeToLive.toMillis())
        );
        try {
            storage.create(BlobInfo.newBuilder(mutexBlobId).setMetadata(metadata).build(),
                    Storage.BlobTargetOption.doesNotExist()
            );
            LOG.trace("Lock acquired. UUID: {}", uuid);
            return true;
        } catch (StorageException e) {
            if ("Precondition Failed".equals(e.getMessage())
                    && "conditionNotMet".equals(e.getReason())) {
                LOG.trace("lost creation race to another parallel process");
                return false;
            } else {
                throw e; // unexpected
            }
        }
    }

    @Override
    public void lock() throws GCSMutexException {
        LOG.trace("lock()");
        for (long i = 0; ; i++) {
            if (tryAcquire()) {
                return;
            } else {
                long waitTimeMs = computeBackoffWaitTimeMs(i);
                LOG.trace("Attempt #{} to acquire lock failed, retrying in {} ms", i + 1, waitTimeMs);
                try {
                    Thread.sleep(waitTimeMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // preserve interrupt status
                    throw new GCSMutexException(e);
                }
            }
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException, GCSMutexException  {
        LOG.trace("lockInterruptibly()");
        for (long i = 0; ; i++) {
            if (tryAcquire()) {
                return;
            } else {
                long waitTimeMs = computeBackoffWaitTimeMs(i);
                LOG.trace("Attempt #{} to acquire lock failed, retrying in {} ms", i + 1, waitTimeMs);
                Thread.sleep(waitTimeMs);
            }
        }
    }

    @Override
    public boolean tryLock() throws GCSMutexException {
        LOG.trace("tryLock()");
        return tryAcquire();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException, GCSMutexException  {
        LOG.trace("tryLock({}, {})", time, unit);
        long start = System.currentTimeMillis();
        for (long i = 0; ; i++) {
            if (tryAcquire()) {
                return true;
            } else {
                long waitTimeMs = computeBackoffWaitTimeMs(i);
                long duration = System.currentTimeMillis() - start;
                if (duration >= unit.toMillis(time)) {
                    LOG.trace("Timeout");
                    return false; // timeout
                }
                long expireMs = unit.toMillis(time) - duration;
                LOG.trace("Attempt #{} to acquire lock failed, retrying in {} ms", i + 1, waitTimeMs);
                Thread.sleep(Math.min(waitTimeMs, expireMs));
            }
        }
    }

    @Override
    public void unlock() throws GCSMutexException {
        LOG.trace("unlock()");
        Blob blob = storage.get(mutexBlobId);
        if (blob == null) {
            LOG.trace("No blob, already unlocked");
            return;
        }
        String uuid = UUID.randomUUID().toString();
        Map<String, String> m = Map.of(
                "uuid", uuid,
                "status", "open"
        );
        storage.update(blob.toBuilder().setMetadata(m).build());
        LOG.trace("Unlocked. UUID: " + uuid);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
