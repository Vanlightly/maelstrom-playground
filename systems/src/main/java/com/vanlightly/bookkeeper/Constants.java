package com.vanlightly.bookkeeper;

import java.util.Arrays;
import java.util.List;

public class Constants {
    public class Metadata {
        public static final String NoLeader = "none";
    }

    public class KeepAlives {
        public static final long KeepAliveIntervalMs = 1000;
        public static final long KeepAliveExpiryMs = 6000;
        public static final long KeepAliveCheckMs = 1000;
    }

    public class Timeouts {
        public static final int TimeoutMs = 5000;
    }

    public static class Bookie {
        public static final int CheckExpiredLongPollReadsIntervalMs = 1000;
    }

    public static class KvStore {
        public static final int CheckLeadershipIntervalMs = 1000;
        public static final int ReaderUpdateMetadataIntervalMs = 1000;
        public static final int MaxMsSinceLastOp = 2000;
        public static final int LongPollTimeoutMs = 4000;
        public static final int LongPollResponseTimeoutMs = 10000;


        public static class Ops {
            public static final String READ = "read";
            public static final String WRITE = "write";
            public static final String CAS = "cas";
            public static final String NOOP = "no-op";

            public static final List<String> Types =
                    Arrays.asList(Ops.READ,
                            Ops.WRITE,
                            Ops.CAS);
        }
    }
}
