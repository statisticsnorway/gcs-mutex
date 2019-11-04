package no.ssb.locking;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class GCSMutex implements Lock {

    private final static Logger LOG = LoggerFactory.getLogger(GCSMutex.class);

    public static GCSMutex create(Storage storage, BlobId mutexBlobId) {
        return new GCSMutex(storage, mutexBlobId, Duration.ofMinutes(30), new Random(), Duration.ofSeconds(60));
    }

    public static GCSMutex create(Storage storage, BlobId mutexBlobId, Duration timeToLive) {
        return new GCSMutex(storage, mutexBlobId, timeToLive, new Random(), Duration.ofSeconds(60));
    }

    public static GCSMutex create(Storage storage, BlobId mutexBlobId, Duration timeToLive, Random backoffRandom, Duration maximumBackoff) {
        return new GCSMutex(storage, mutexBlobId, timeToLive, backoffRandom, maximumBackoff);
    }

    public static GCSMutex create(Path serviceAccountKeyPath, String bucket, String path) {
        return new GCSMutex(storageFrom(serviceAccountKeyPath), BlobId.of(bucket, path), Duration.ofMinutes(30), new Random(), Duration.ofSeconds(60));
    }

    public static GCSMutex create(Path serviceAccountKeyPath, String bucket, String path, Duration timeToLive) {
        return new GCSMutex(storageFrom(serviceAccountKeyPath), BlobId.of(bucket, path), timeToLive, new Random(), Duration.ofSeconds(60));
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
        return Math.min(1000 * (1 << Math.min(i, 8)) + backoffRandom.nextInt(1001), maximumBackoff.toMillis());
    }

    @Override
    public void lock() {
        LOG.debug("lock()");
        for (long i = 0; ; i++) {
            try {
                storage.create(BlobInfo.newBuilder(mutexBlobId).setContentType("application/json").build(), Storage.BlobTargetOption.doesNotExist());
                return; // lock acquired
            } catch (StorageException e) {
                long waitTimeMs = computeBackoffWaitTimeMs(i);
                LOG.trace("Attempt #{} to acquire lock failed, retrying in {} ms", i + 1, waitTimeMs);
                try {
                    Thread.sleep(waitTimeMs);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt(); // preserve interrupt status
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        LOG.debug("lockInterruptibly()");
        for (long i = 0; ; i++) {
            try {
                storage.create(BlobInfo.newBuilder(mutexBlobId).setContentType("application/json").build(), Storage.BlobTargetOption.doesNotExist());
                return; // lock acquired
            } catch (StorageException e) {
                long waitTimeMs = computeBackoffWaitTimeMs(i);
                LOG.trace("Attempt #{} to acquire lock failed, retrying in {} ms", i + 1, waitTimeMs);
                Thread.sleep(waitTimeMs);
            }
        }
    }

    @Override
    public boolean tryLock() {
        LOG.debug("tryLock()");
        try {
            storage.create(BlobInfo.newBuilder(mutexBlobId).setContentType("application/json").build(), Storage.BlobTargetOption.doesNotExist());
            return true;
        } catch (StorageException e) {
            return false;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        LOG.debug("tryLock({}, {})", time, unit);
        long start = System.currentTimeMillis();
        for (long i = 0; ; i++) {
            try {
                storage.create(BlobInfo.newBuilder(mutexBlobId).setContentType("application/json").build(), Storage.BlobTargetOption.doesNotExist());
                return true; // lock acquired
            } catch (StorageException e) {
                long waitTimeMs = computeBackoffWaitTimeMs(i);
                long duration = System.currentTimeMillis() - start;
                if (duration >= unit.toMillis(time)) {
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
        LOG.debug("unlock()");
        storage.delete(mutexBlobId);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
