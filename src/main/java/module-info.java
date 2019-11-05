module no.ssb.mutex.gcs {

    requires org.slf4j;
    requires google.cloud.core;
    requires google.cloud.storage;
    requires com.google.auth.oauth2;
    requires com.google.auth;

    exports no.ssb.locking;
}