package no.ssb.locking;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Optional.ofNullable;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class GCSMetadataMutexTest {

    Storage storage;
    String bucket;
    BlobId blobId;
    GCSMetadataMutex mutex;

    @BeforeMethod
    public void setup() {
        Path pathToGcsSaKeyFile = Path.of(ofNullable(System.getenv("GCS_MUTEX_SERVICE_ACCOUNT_KEY_FILE")).orElse("secret/gcs_sa.json"));
        storage = GCSMetadataMutex.storageFrom(pathToGcsSaKeyFile);
        bucket = ofNullable(System.getenv("GCS_MUTEX_BUCKET")).orElse("bip-drone-dependency-cache");
        blobId = BlobId.of(bucket, "testng/mutex-" + UUID.randomUUID().toString() + ".dat");
        mutex = GCSMetadataMutex.create(storage, blobId);
    }

    @Test
    public void testLock() {
        mutex.lock();
        assertFalse(mutex.tryLock());
    }

    @Test
    public void testLockInterruptibly() throws InterruptedException {
        mutex.lockInterruptibly();
        assertFalse(mutex.tryLock());
    }

    @Test
    public void testTryLock() {
        assertTrue(mutex.tryLock());
        assertFalse(mutex.tryLock());
    }

    @Test
    public void testTryLockTimeout() throws InterruptedException {
        assertTrue(mutex.tryLock(30, TimeUnit.SECONDS));
        assertFalse(mutex.tryLock(10, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testUnlock() {
        assertTrue(mutex.tryLock());
        mutex.unlock();
        assertTrue(mutex.tryLock());
    }

    @Test
    public void testUnlockAsFirstOperation() {
        mutex.unlock();
        assertTrue(mutex.tryLock());
    }

    @Test
    public void thatTimeToLiveWorks() throws InterruptedException {
        GCSMetadataMutex mutex = GCSMetadataMutex.create(storage, blobId, Duration.ofMillis(10)); // use shorter ttl
        assertTrue(mutex.tryLock());
        Thread.sleep(500);
        assertTrue(mutex.tryLock()); // should be allowed because the ttl of 10 milliseconds should now have expired
    }
}