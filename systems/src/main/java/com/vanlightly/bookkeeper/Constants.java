package com.vanlightly.bookkeeper;

public class Constants {
    public class Metadata {
        public static final String NoLeader = "none";
    }

    public class KeepAlives {
        public static final long KeepAliveIntervalMs = 60000;
        public static final long KeepAliveExpiryMs = 300000;
        public static final long KeepAliveCheckMs = 10000;
    }

    public class Timeouts {
        public static final int TimeoutMs = 6000;
    }

    public class KvStore {
        public static final int CheckLeadershipIntervalMs = 1000;
        public static final int ReaderUpdateMetadataIntervalMs = 1000;


        public class Ops {
            public static final String READ = "read";
            public static final String WRITE = "write";
            public static final String CAS = "cas";
        }
    }
}
