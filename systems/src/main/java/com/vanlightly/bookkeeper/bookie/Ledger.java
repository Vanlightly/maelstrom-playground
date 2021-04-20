package com.vanlightly.bookkeeper.bookie;

import com.vanlightly.bookkeeper.Logger;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Ledger {
    Logger logger;
    long ledgerId;
    Map<Long, String> entries;
    boolean isFenced;
    long lac;
    NavigableMap<Long, List<LongPollLacRead>> lacLongPollReads;

    public Ledger(Logger logger, long ledgerId) {
        this.logger = logger;
        this.ledgerId = ledgerId;
        this.entries = new HashMap<>();
        this.isFenced = false;
        this.lac = -1L;
        this.lacLongPollReads = new TreeMap<>();
    }

    public void add(Long entryId, Long lac, String value) {
        entries.put(entryId, value);
        logger.logDebug("Adding entry=" + entryId + ", lac=" + lac + ", value=" + value);

        if (lac > this.lac) {
            this.lac = lac;
        }

        NavigableMap<Long, List<LongPollLacRead>> completableLpReads = lacLongPollReads.headMap(lac, true);
        for (Map.Entry<Long, List<LongPollLacRead>> lpReads : completableLpReads.entrySet()) {
            lpReads.getValue().stream().forEach((LongPollLacRead lpRead) -> {
                logger.logDebug("Long poll read response at entry: " + lpReads.getKey());
                String val = read(lpRead.previousLac + 1);
                lpRead.getFuture().complete(new Entry(ledgerId,
                        lpRead.previousLac + 1,
                        this.lac,
                        val));
            });
            lacLongPollReads.remove(lpReads.getKey());
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

    public void addLacFuture(long previousLac, int timeoutMs, CompletableFuture<Entry> future) {
        if (previousLac < this.lac) {
            String val = read(previousLac + 1);
            future.complete(new Entry(ledgerId,
                    previousLac + 1,
                    this.lac,
                    val));
        } else {
            lacLongPollReads.compute(previousLac, (Long k, List<LongPollLacRead> v) -> {
                if (v == null) {
                    v = new ArrayList<>();
                }
                v.add(new LongPollLacRead(future, previousLac, timeoutMs));
                return v;
            });
        }
    }

    public boolean expireLacLongPollReads() {
        Instant now = Instant.now();

        List<LongPollLacRead> expiredReads =
                lacLongPollReads.entrySet()
                .stream()
                .flatMap((e) -> e.getValue()
                                    .stream()
                                    .filter(x -> x.hasExpired(now)))
                .collect(Collectors.toList());

        if (expiredReads.isEmpty()) {
            return false;
        } else {
            for (LongPollLacRead expiredRead : expiredReads) {
                long entryId = expiredRead.getPreviousLac();
                expiredRead.getFuture().complete(new Entry(ledgerId,
                        entryId,
                        this.lac,
                        null));
                lacLongPollReads.get(expiredRead.getPreviousLac()).remove(expiredRead);
                logger.logDebug("Long poll read timeout at entry: " + entryId);
            }

            return true;
        }
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
        private Instant deadline;

        public LongPollLacRead(CompletableFuture<Entry> future, long previousLac, int timeoutMs) {
            this.future = future;
            this.previousLac = previousLac;
            this.deadline = Instant.now().plus(timeoutMs, ChronoUnit.MILLIS);
        }

        public CompletableFuture<Entry> getFuture() {
            return future;
        }

        public long getPreviousLac() {
            return previousLac;
        }

        public boolean hasExpired(Instant now) {
            return now.isAfter(deadline);
        }
    }


}
