package com.vanlightly.bookkeeper.kv.bkclient;

import com.vanlightly.bookkeeper.kv.log.Position;

public class Entry {
    long ledgerId;
    long entryId;
    String value;

    public Entry(long ledgerId, long entryId, String value) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.value = value;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public String getValue() {
        return value;
    }

    public Position getPosition() {
        return new Position(ledgerId, entryId);
    }
}
