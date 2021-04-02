package com.vanlightly.bookkeeper.metadata;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class LedgerMetadata {
    long ledgerId;
    int writeQuorum;
    int ackQuorum;
    NavigableMap<Long, List<String>> ensembles;
    long lastEntryId;
    LedgerStatus status;

    public LedgerMetadata() {
    }

    public LedgerMetadata(long ledgerId,
                          int writeQuorum,
                          int ackQuorum,
                          List<String> firstEnsemble) {
        this.ledgerId = ledgerId;
        this.writeQuorum = writeQuorum;
        this.ackQuorum = ackQuorum;
        this.ensembles = new TreeMap<>();
        this.ensembles.put(0L, firstEnsemble);
        this.status = LedgerStatus.OPEN;
    }

    public LedgerMetadata(LedgerMetadata md) {
        this.ledgerId = md.getLedgerId();
        this.writeQuorum = md.getWriteQuorum();
        this.ackQuorum = md.getAckQuorum();
        this.ensembles = new TreeMap<>(md.getEnsembles());
        this.status = md.getStatus();
    }

    public void addEnsemble(Long entryId, List<String> ensemble) {
        ensembles.put(entryId, ensemble);
    }

    public void setLastEntryId(Long lastEntryId) {
        this.lastEntryId = lastEntryId;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public void setLedgerId(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    public int getWriteQuorum() {
        return writeQuorum;
    }

    public int getAckQuorum() {
        return ackQuorum;
    }

    public NavigableMap<Long, List<String>> getEnsembles() {
        return ensembles;
    }

    public void setEnsembles(NavigableMap<Long, List<String>> ensembles) {
        this.ensembles = ensembles;
    }

    public List<String> getCurrentEnsemble() {
        return ensembles.lastEntry().getValue();
    }

    public void replaceCurrentEnsemble(List<String> ensemble) {
        ensembles.put(ensembles.lastKey(), ensemble);
    }

    public long getLastEntryId() {
        return lastEntryId;
    }

    public void setLastEntryId(long lastEntryId) {
        this.lastEntryId = lastEntryId;
    }

    public LedgerStatus getStatus() {
        return status;
    }

    public void setStatus(LedgerStatus status) {
        this.status = status;
    }
}
