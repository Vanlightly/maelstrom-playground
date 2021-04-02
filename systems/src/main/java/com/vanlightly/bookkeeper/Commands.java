package com.vanlightly.bookkeeper;

public class Commands {
    public static class Bookie {
        public final static String ADD_ENTRY = "add-entry";
        public final static String READ_ENTRY = "read-entry";
        public final static String READ_LAC = "read-lac";
        public final static String RECOVERY_READ_ENTRY = "recovery-read-entry";
        public final static String RECOVERY_ADD_ENTRY = "recovery-add-entry";
    }

    public static class Metadata {
        public final static String SESSION_NEW = "session-new";
        public final static String SESSION_KEEP_ALIVE = "session-ka";
        public final static String SESSION_EXPIRED = "session-expired";

        // KV Store metadata
        public final static String GET_LEADER_ID = "get-leader";
        public final static String GET_LEDGER_LIST = "get-ledger-list";
        public final static String LEDGER_LIST_UPDATE = "update-ledger-list";
        public final static String CURSOR_UPDATE = "update-cursor";

        // BK metadata
        public final static String BK_METADATA_READ = "read-bk-metadata";
        public final static String LEDGER_READ = "read-ledger";
        public final static String LEDGER_UPDATE = "update-ledger";
        public final static String LEDGER_CREATE = "create-ledger";
    }

    public static class Client {
        public final static String WRITE = "write";
        public final static String READ = "read";
    }
}
