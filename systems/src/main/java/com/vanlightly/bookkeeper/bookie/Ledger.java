package com.vanlightly.bookkeeper.bookie;

import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;
import com.vanlightly.bookkeeper.ReturnCodes;
import com.vanlightly.bookkeeper.kv.bkclient.BkException;
import com.vanlightly.bookkeeper.util.DeadlineCollection;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Ledger {
    Logger logger = LogManager.getLogger(this.getClass().getName());
    long ledgerId;
    Map<Long, String> entries;
    boolean isFenced;
    long lac;
    DeadlineCollection<LongPollLacRead> lacLongPollReadsByLac;
    DeadlineCollection<LongPollLacRead> lacLongPollReadsByTimeout;

    public Ledger(long ledgerId) {
        this.ledgerId = ledgerId;
        this.entries = new HashMap<>();
        this.isFenced = false;
        this.lac = -1L;
        this.lacLongPollReadsByLac = new DeadlineCollection<>();
        this.lacLongPollReadsByTimeout = new DeadlineCollection<>();
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public void add(Long entryId, Long entryLac, String value) {
        entries.put(entryId, value);
        logger.logDebug("Adding entry=" + entryId + ", lac=" + entryLac + ", value=" + value);

        if (entryLac > this.lac) {
            this.lac = entryLac;
        }

        while (lacLongPollReadsByLac.hasNext(entryLac)) {
            LongPollLacRead lpRead = lacLongPollReadsByLac.next();
            logger.logDebug("Long poll read response by LAC: " + entryLac);
            String val = read(lpRead.previousLac + 1);
            lpRead.getFuture().complete(new Entry(ledgerId,
                    lpRead.previousLac + 1,
                    this.lac,
                    val));
            lpRead.makeInactive();
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

    public void fenceLedger() {
        isFenced = true;

        for (LongPollLacRead lpRead : lacLongPollReadsByLac.getAll()) {
            if (lpRead.isActive()) {
                lpRead.getFuture().completeExceptionally(
                        new BkException("Ledger fenced", ReturnCodes.Bookie.FENCED));
            }
        }

        lacLongPollReadsByLac.clear();
        lacLongPollReadsByTimeout.clear();
    }

    public long getLac() {
        return lac;
    }

    public void setLac(long lac) {
        this.lac = lac;
    }

    public void addLacFuture(long previousLac, int timeoutMs, CompletableFuture<Entry> future) {
        if (previousLac < this.lac) {
            String val = read(previousLac + 1);
            future.complete(new Entry(ledgerId,
                    previousLac + 1,
                    this.lac,
                    val));
        } else {
            LongPollLacRead lpRead = new LongPollLacRead(future, previousLac);
            lacLongPollReadsByLac.add(previousLac + 1, lpRead);
            lacLongPollReadsByTimeout.add(System.currentTimeMillis() + timeoutMs, lpRead);
        }
    }

    public boolean expireLacLongPollReads() {
        long now = System.currentTimeMillis();

        boolean foundExpired = false;

        while (lacLongPollReadsByTimeout.hasNext(now)) {
            LongPollLacRead expiredRead = lacLongPollReadsByTimeout.next();
            if (expiredRead.isActive()) {
                long entryId = expiredRead.getPreviousLac();
                expiredRead.getFuture().complete(new Entry(ledgerId,
                        entryId,
                        this.lac,
                        null));
                expiredRead.makeInactive();
                logger.logDebug("Long poll read timeout at entry: " + entryId);

                foundExpired = true;
            }
        }

        return foundExpired;
    }

    public Map<Long, String> getEntries() {
        return entries;
    }

    @Override
    public String toString() {
        return "Ledger{" +
                "ledgerId=" + ledgerId +
                ", entries=" + entries +
                ", isFenced=" + isFenced +
                ", lac=" + lac +
                '}';
    }

    private static class LongPollLacRead {
        private CompletableFuture<Entry> future;
        private long previousLac;
        private boolean active;

        public LongPollLacRead(CompletableFuture<Entry> future, long previousLac) {
            this.future = future;
            this.previousLac = previousLac;
            this.active = true;
        }

        public CompletableFuture<Entry> getFuture() {
            return future;
        }

        public long getPreviousLac() {
            return previousLac;
        }

        public boolean isActive() {
            return active;
        }

        public void makeInactive() {
            active = false;
        }
    }


}
