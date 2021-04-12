package com.vanlightly.bookkeeper.kv.log;

public enum ReaderState {
    NO_LEDGER,
    PENDING_LEDGER,
    READING,
    IN_LONG_POLL,
    IDLE,
    CLOSED
}
