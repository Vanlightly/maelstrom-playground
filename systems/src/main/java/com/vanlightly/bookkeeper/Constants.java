package com.vanlightly.bookkeeper;

import java.util.Arrays;
import java.util.List;

public class Constants {
    public class Metadata {
        public static final String NoLeader = "none";
    }

    public class KeepAlives {
        public static final long LoseKeepAlivesAfterMs = 2500;
        public static final long KeepAliveIntervalMs = 200;
        public static final long KeepAliveExpiryMs = 1500;
        public static final long KeepAliveCheckMs = 100;
    }

    public class Timeouts {
        public static final int TimeoutMs = 500;
        public static final int ProxyTimeoutMs = 2000;
    }

    public static class Bookie {
        public static final int CheckExpiredLongPollReadsIntervalMs = 2000;
        public static final int WriteQuorum = 3;
        public static final int AckQuorum = 2;
        public static int BookieCount = 3; // default is 3 but is configurable.
    }

    public static class KvStore {
        public static final int CheckLeadershipIntervalMs = 2000;
        public static final int ReaderUpdateMetadataIntervalMs = 2000;
        public static final int MaxMsSinceLastOp = 1000;
        public static final int LongPollTimeoutMs = 1000;
        public static final int LongPollResponseTimeoutMs = 2000;


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
