module no.ssb.mutex.gcs {

    requires org.slf4j;
    requires google.cloud.storage;
    requires com.google.auth.oauth2;

    exports no.ssb.locking;
}