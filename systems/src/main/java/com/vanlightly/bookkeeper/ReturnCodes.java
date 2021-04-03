package com.vanlightly.bookkeeper;

public class ReturnCodes {
    public final static String OK = "ok";
    public final static String TIME_OUT = "timeout";

    public static class Bookie {
        public final static String FENCED = "fenced";
        public final static String NO_SUCH_LEDGER = "no-such-ledger";
        public final static String NO_SUCH_ENTRY = "no-such-entry";
        public final static String NOT_ENOUGH_BOOKIES = "not-enough-bookies";
        public final static String NOT_ENOUGH_EXPLICIT_RESPONSES = "not-enough-explicit-responses";
    }

    public static class Ledger {
        public final static String LEDGER_CLOSED = "ledger-closed";
        public final static String LESS_THAN_ACK_QUORUM = "less-than-quorum";
    }

    public static class Metadata {
        public final static String NO_SUCH_LEDGER = "no-such-ledger";
        public final static String BAD_VERSION = "bad-version";
        public final static String BAD_SESSION = "bad-session";
        public final static String LEADER_EXISTS = "leader-exists";

    }
}
