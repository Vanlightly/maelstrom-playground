package com.vanlightly.bookkeeper.kv.log;

public enum ReaderState {
    NO_LEDGER,
    PENDING_LEDGER,
    READING,
    IDLE,
    CLOSED
}
