package com.vanlightly.bookkeeper.bookie;

import java.util.HashMap;
import java.util.Map;

public class Ledger {
    Map<Long, String> entries;
    boolean isFenced;
    long lac;

    public Ledger() {
        entries = new HashMap<>();
        isFenced = false;
        lac = -1L;
    }

    public void add(Long entryId, Long lac, String value) {
        entries.put(entryId, value);

        if (lac > this.lac) {
            this.lac = lac;
        }
    }

    public boolean hasEntry(Long entryId) {
        return entries.containsKey(entryId);
    }

    public String read(Long entryId) {
        return entries.get(entryId);
    }

    public boolean isFenced() {
        return isFenced;
    }

    public void setFenced(boolean fenced) {
        isFenced = fenced;
    }

    public long getLac() {
        return lac;
    }

    public void setLac(long lac) {
        this.lac = lac;
    }
}
