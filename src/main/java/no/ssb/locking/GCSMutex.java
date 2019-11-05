package no.ssb.locking;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * This class implements mutual-exclusion through GCS object-data and preconditions. Although this class implements the
 * java.util.concurrent.Lock interface, this class will not ensure any happens-before thread-safety effects.
 * Due to limitations of if-generation-match:0 precondition, see https://cloud.google.com/storage/docs/generations-preconditions
 * it could be unsafe to delete the file in order to implement the unlock operation.
 * <p>
 * This object-data based mutex requires read-write object access to the relevant object in the bucket.
 */
public class GCSMutex implements Lock {

    private final static Logger LOG = LoggerFactory.getLogger(GCSMutex.class);

    public static GCSMutex create(Storage storage, BlobId mutexBlobId) {
        return new GCSMutex(storage, mutexBlobId, Duration.ofMinutes(1), new Random(), Duration.ofSeconds(64));
    }

    public static GCSMutex create(Storage storage, BlobId mutexBlobId, Duration timeToLive) {
        return new GCSMutex(storage, mutexBlobId, timeToLive, new Random(), Duration.ofSeconds(64));
    }

    public static GCSMutex create(Storage storage, BlobId mutexBlobId, Duration timeToLive, Random backoffRandom, Duration maximumBackoff) {
        return new GCSMutex(storage, mutexBlobId, timeToLive, backoffRandom, maximumBackoff);
    }

    public static GCSMutex create(Path serviceAccountKeyPath, String bucket, String path) {
        return new GCSMutex(storageFrom(serviceAccountKeyPath), BlobId.of(bucket, path), Duration.ofMinutes(1), new Random(), Duration.ofSeconds(64));
    }

    public static GCSMutex create(Path serviceAccountKeyPath, String bucket, String path, Duration timeToLive) {
        return new GCSMutex(storageFrom(serviceAccountKeyPath), BlobId.of(bucket, path), timeToLive, new Random(), Duration.ofSeconds(64));
    }

    public static GCSMutex create(Path serviceAccountKeyPath, String bucket, String path, Duration timeToLive, Random backoffRandom, Duration maximumBackoff) {
        return new GCSMutex(storageFrom(serviceAccountKeyPath), BlobId.of(bucket, path), timeToLive, backoffRandom, maximumBackoff);
    }

    public static Storage storageFrom(Path serviceAccountKeyPath) {
        ServiceAccountCredentials sourceCredentials;
        try {
            sourceCredentials = ServiceAccountCredentials.fromStream(Files.newInputStream(serviceAccountKeyPath, StandardOpenOption.READ));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        GoogleCredentials scopedCredentials = sourceCredentials.createScoped(Arrays.asList("https://www.googleapis.com/auth/devstorage.read_write"));
        Storage storage = StorageOptions.newBuilder().setCredentials(scopedCredentials).build().getService();
        return storage;
    }

    final Storage storage;
    final BlobId mutexBlobId;
    final Duration timeToLive;
    final Duration maximumBackoff;
    final Random backoffRandom;

    public GCSMutex(Storage storage, BlobId mutexBlobId, Duration timeToLive, Random backoffRandom, Duration maximumBackoff) {
        this.storage = storage;
        this.mutexBlobId = mutexBlobId;
        this.timeToLive = timeToLive;
        this.maximumBackoff = maximumBackoff;
        this.backoffRandom = backoffRandom;
    }

    long computeBackoffWaitTimeMs(long i) {
        return Math.min(1000 * (1 << Math.min(i, 8)), maximumBackoff.toMillis()) + backoffRandom.nextInt(1001);
    }

    boolean tryAcquire() {
        Blob blob = storage.get(mutexBlobId);
        if (blob == null) {
            return acquireByCreatingFileIfDoesNotExist(UUID.randomUUID().toString(), "locked", timeToLive.toMillis());
        }
        if (blob.getSize() > 1024) {
            throw new RuntimeException("Lock content too large, cannot exceed 1024 bytes.");
        }
        byte[] array = new byte[blob.getSize().intValue()];
        try (ReadChannel reader = blob.reader()) {
            ByteBuffer bb = ByteBuffer.wrap(array);
            int n;
            while ((n = reader.read(bb)) != -1) {
                Thread.sleep(100); // avoid excessive cpu usage when socket read is not yet ready
            }
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        Pattern pattern = Pattern.compile("\\s*([^\\s:]+):\\s*([^\\s]*)\\s*");
        Map<String, String> contentMap = new String(array, StandardCharsets.UTF_8).lines().collect(Collectors.toMap(
                line -> {
                    Matcher m = pattern.matcher(line);
                    if (!m.matches()) {
                        throw new RuntimeException("Line does not match pattern. Line: " + line);
                    }
                    return m.group(1);
                },
                line -> {
                    Matcher m = pattern.matcher(line);
                    if (!m.matches()) {
                        throw new RuntimeException("Line does not match pattern. Line: " + line);
                    }
                    return m.group(2);
                }
                )
        );

        String status = contentMap.get("status");
        if (!"locked".equalsIgnoreCase(status)) {
            LOG.trace("Mutex available, attempting to acquire lock...");
            return updateMutexThroughDataOverwrite(blob, UUID.randomUUID().toString(), "locked", timeToLive.toMillis());
        }

        String uuid = contentMap.get("uuid");

        // already locked check whether lock has expired according to its defined ttl
        String existingTimeToLive = contentMap.get("time-to-live");
        if (existingTimeToLive == null) {
            LOG.warn("Failed to acquire lock, existing lock has no expiry set: {}", uuid);
            return false;
        }
        long ettl = Long.parseLong(existingTimeToLive);
        long lastModified = blob.getUpdateTime();
        long expiry = lastModified + ettl;
        long expiredDuration = System.currentTimeMillis() - expiry;
        if (expiredDuration >= 0) {
            LOG.trace("Mutex expired, attempting to acquire lock...");
            return updateMutexThroughDataOverwrite(blob, UUID.randomUUID().toString(), "locked", timeToLive.toMillis());
        }
        LOG.trace("Failed to acquire lock, already held by someone else with uuid: {} and ttl: {}", uuid, ettl);
        return false;
    }

    boolean updateMutexThroughDataOverwrite(Blob blob, String uuid, String status, long ttlMs) {
        try {
            writeBytesToBlobIfGenerationMatch(blob, uuid, status, ttlMs);
            LOG.trace("Lock acquired. UUID: " + uuid);
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

    void writeBytesToBlobIfGenerationMatch(Blob blob, String uuid, String status, long ttlMs) {
        StringBuilder sb = new StringBuilder();
        sb.append("uuid: ").append(uuid).append("\n");
        sb.append("status: ").append(status).append("\n");
        sb.append("time-to-live: ").append(ttlMs).append("\n");
        ByteBuffer bb = ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8));
        try (WriteChannel ch = storage.writer(blob.toBuilder().build(), Storage.BlobWriteOption.generationMatch())) {
            ch.setChunkSize(1024);
            int i = 0;
            while (bb.hasRemaining()) {
                if (i >= 25) {
                    throw new RuntimeException("Unable to write data to GCS object");
                }
                if ((i + 1) % 2 == 0) {
                    // avoid excessive cpu usage while retrying socket write
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                int n = ch.write(bb);
                i++;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    boolean acquireByCreatingFileIfDoesNotExist(String uuid, String status, long ttlMs) {
        // lock-file does not exist
        StringBuilder sb = new StringBuilder();
        sb.append("uuid: ").append(uuid).append("\n");
        sb.append("status: ").append(status).append("\n");
        sb.append("time-to-live: ").append(ttlMs).append("\n");
        try {
            storage.create(BlobInfo.newBuilder(mutexBlobId).build(),
                    sb.toString().getBytes(StandardCharsets.UTF_8),
                    Storage.BlobTargetOption.doesNotExist()
            );
            LOG.trace("Lock acquired. UUID: " + uuid);
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
    public void lock() {
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
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
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
    public boolean tryLock() {
        LOG.trace("tryLock()");
        return tryAcquire();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
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
    public void unlock() {
        LOG.trace("unlock()");
        Blob blob = storage.get(mutexBlobId);
        if (blob == null) {
            LOG.trace("No blob, already unlocked");
            return;
        }
        String uuid = UUID.randomUUID().toString();
        writeBytesToBlobIfGenerationMatch(blob, uuid, "unlocked", timeToLive.toMillis());
        LOG.trace("Unlocked. UUID: " + uuid);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
