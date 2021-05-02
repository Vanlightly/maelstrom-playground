package com.vanlightly.bookkeeper;

public class Fields {
    public final static String SOURCE = "src";
    public final static String DEST = "dest";
    public final static String IN_REPLY_TO = "in_reply_to";
    public final static String MSG_ID = "msg_id";
    public final static String BODY = "body";
    public final static String MSG_TYPE = "type";
    public final static String SESSION_ID = "session_id";
    public final static String RC = "rc";
    public final static String VERSION = "version";

    public class L {
        public final static String LEDGER_ID = "ledger_id";
        public final static String ENTRY_ID = "entry_id";
        public final static String VALUE = "op";
        public final static String LAC = "lac";
        public final static String PREVIOUS_LAC = "previous-lac";
        public final static String LONG_POLL_TIMEOUT_MS = "timeout-ms";
        public final static String RECOVERY = "recovery";
        public final static String FENCE = "fence";
    }

    public class M {
        public final static String LEDGER_METADATA = "ledger_metadata";
    }

    public class KV {
        public final static String LEDGER_LIST_VERSION = "ledger_list_version";
        public final static String LEDGER_LIST = "ledger_list";
        public final static String LEADER = "leader";
        public final static String LEADER_VERSION = "leader_version";

        public class Op {
            public final static String TYPE = "type";
            public final static String KEY = "key";
            public final static String VALUE = "value";
            public final static String FROM = "from";
            public final static String TO = "to";
            public final static String CODE = "code";
            public final static String ERROR_TEXT = "text";
            public final static String CMD_TYPE = "cmd-type";
        }

    }

}
