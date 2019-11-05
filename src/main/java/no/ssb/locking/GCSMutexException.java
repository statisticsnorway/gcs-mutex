package no.ssb.locking;

public class GCSMutexException extends RuntimeException {
    public GCSMutexException() {
    }

    public GCSMutexException(String message) {
        super(message);
    }

    public GCSMutexException(String message, Throwable cause) {
        super(message, cause);
    }

    public GCSMutexException(Throwable cause) {
        super(cause);
    }

    public GCSMutexException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
