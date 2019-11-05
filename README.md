# gcs-mutex
Mutual exclusion locking java client implementation backed by Google Cloud Storage

### Exmple of use
```java
import no.ssb.locking.GCSMetadataMutex;
import no.ssb.locking.GCSMutex;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.locks.Lock;

class Sample {

    final Path serviceAccountKeyPath = Path.of("/path/to/gcs-sa-key.json");
    final String bucket = "my-bucket";

    void useMutex() {
        Lock mutex = GCSMutex.create(serviceAccountKeyPath, bucket, "folder/data-mutex-4fc2d86b.lck", Duration.ofSeconds(90));
        mutex.lock();
        try {
            /*
             * perform operations protected by GCS data mutex here
             */
        } finally {
            mutex.unlock();
        }
    }

    void useMetaMutex() {
        Lock mutex = GCSMetadataMutex.create(serviceAccountKeyPath, bucket, "folder/meta-mutex-9b33fa10.lck", Duration.ofSeconds(60));
        mutex.lock();
        try {
            /*
             * perform operations protected by GCS metadata mutex here
             */
        } finally {
            mutex.unlock();
        }
    }
}

```