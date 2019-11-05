package no.ssb.locking;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Optional.ofNullable;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class GCSMutexTest {

    Storage storage;
    String bucket;
    String prefix;

    BlobId blobId;
    GCSMutex mutex;

    @BeforeClass
    public void beforeClass() {
        Path pathToGcsSaKeyFile = Path.of(ofNullable(System.getenv("GCS_MUTEX_SERVICE_ACCOUNT_KEY_FILE")).orElse("secret/gcs_sa.json"));
        storage = GCSMutex.storageFrom(pathToGcsSaKeyFile);
        bucket = ofNullable(System.getenv("GCS_MUTEX_BUCKET")).orElse("bip-drone-dependency-cache");
        prefix = "testng-" + UUID.randomUUID().toString();
        clearBucketFolder();
    }

    @AfterClass
    public void clearBucketFolder() {
        Page<Blob> page = storage.list(bucket, Storage.BlobListOption.prefix(prefix + "/"));
        BlobId[] blobs = StreamSupport.stream(page.iterateAll().spliterator(), false).map(BlobInfo::getBlobId).collect(Collectors.toList()).toArray(new BlobId[0]);
        if (blobs.length > 0) {
            List<Boolean> deletedList = storage.delete(blobs);
            for (Boolean deleted : deletedList) {
                if (!deleted) {
                    throw new RuntimeException("Unable to delete blob in bucket");
                }
            }
        }
    }


    @BeforeMethod
    public void setup() {
        blobId = BlobId.of(bucket, prefix + "/mutex-" + UUID.randomUUID().toString() + ".dat");
        mutex = GCSMutex.create(storage, blobId);
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
    public void thatAcquireByCreatingFileIfDoesNotExistWorksBothWhenFileDoesNotExistsAndWhenItDoesExists() {
        assertTrue(mutex.acquireByCreatingFileIfDoesNotExist(UUID.randomUUID().toString(), "custom-test-status", 12345));
        // file now exists, the second attempt to create file should fail
        assertFalse(mutex.acquireByCreatingFileIfDoesNotExist(UUID.randomUUID().toString(), "custom-test-status-2", 54321));
    }

    @Test
    public void thatUpdateMutexThroughDataOverwriteWorksBothWhenGenerationMatchesAndWhenNot() {
        Blob blob = storage.create(BlobInfo.newBuilder(blobId).build()); // create empty file, should now be generation 1
        assertTrue(mutex.updateMutexThroughDataOverwrite(blob, UUID.randomUUID().toString(), "custom", 12345));
        assertFalse(mutex.updateMutexThroughDataOverwrite(blob, UUID.randomUUID().toString(), "custom2", 54321)); // fails because generation is now 2
    }

    @Test
    public void thatReadMutexContentWorksForNewlyCreatedLock() {
        assertTrue(mutex.tryAcquire());
        Map<String, String> map = mutex.readMutexContent(storage.get(blobId));
        assertTrue(map.containsKey("status"));
        assertTrue(map.containsKey("uuid"));
        assertTrue(map.containsKey("time-to-live"));
    }

    @Test
    public void thatTryAcquireWillGetLockWhenNoLockfileExists() {
        assertTrue(mutex.tryAcquire());
    }

    @Test
    public void thatTryAcquireWillGetLockWhenExistingLockfileIsExpired() throws InterruptedException {
        GCSMutex mutex = GCSMutex.create(storage, blobId, Duration.ofMillis(10)); // use shorter ttl
        assertTrue(mutex.tryAcquire());
        Thread.sleep(500);
        assertTrue(mutex.tryAcquire()); // should be allowed because the ttl of 10 milliseconds should now have expired
    }
}