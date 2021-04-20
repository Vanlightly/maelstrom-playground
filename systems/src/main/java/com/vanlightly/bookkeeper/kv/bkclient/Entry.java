package com.vanlightly.bookkeeper.kv.bkclient;

import com.vanlightly.bookkeeper.kv.log.Position;

public class Entry {
    long ledgerId;
    long entryId;
    long lac;
    String value;

    public Entry(long ledgerId, long entryId, String value) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.value = value;
    }

    public Entry(long ledgerId, long entryId, long lac, String value) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.lac = lac;
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

    public long getLac() {
        return lac;
    }

    public Position getPosition() {
        return new Position(ledgerId, entryId);
    }

    @Override
    public String toString() {
        if (value != null) {
            return "Entry{" +
                    "ledgerId=" + ledgerId +
                    ", entryId=" + entryId +
                    ", lac=" + lac +
                    ", value=" + value +
                    '}';
        } else {
            return "Entry{" +
                    "ledgerId=" + ledgerId +
                    ", entryId=" + entryId +
                    ", lac=" + lac +
                    '}';
        }
    }
}
