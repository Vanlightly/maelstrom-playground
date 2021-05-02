package com.vanlightly.bookkeeper.metadata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

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
        this.lastEntryId = -1L;
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
        this.lastEntryId = -1L;
    }

    public LedgerMetadata(LedgerMetadata md) {
        this.ledgerId = md.getLedgerId();
        this.writeQuorum = md.getWriteQuorum();
        this.ackQuorum = md.getAckQuorum();
        this.ensembles = new TreeMap<>(md.getEnsembles());
        this.status = md.getStatus();
    }

    @JsonProperty(value = "ledgerId")
    public long getLedgerId() {
        return ledgerId;
    }

    public void setLedgerId(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    @JsonProperty(value = "writeQuorum")
    public int getWriteQuorum() {
        return writeQuorum;
    }

    public void setWriteQuorum(int writeQuorum) {
        this.writeQuorum = writeQuorum;
    }

    @JsonProperty(value = "ackQuorum")
    public int getAckQuorum() {
        return ackQuorum;
    }

    public void setAckQuorum(int ackQuorum) {
        this.ackQuorum = ackQuorum;
    }

    @JsonProperty(value = "ensembles")
    public NavigableMap<Long, List<String>> getEnsembles() {
        return ensembles;
    }

    public void setEnsembles(NavigableMap<Long, List<String>> ensembles) {
        this.ensembles = ensembles;
    }

    @JsonProperty(value = "lastEntryId")
    public long getLastEntryId() {
        return lastEntryId;
    }

    public void setLastEntryId(long lastEntryId) {
        this.lastEntryId = lastEntryId;
    }

    @JsonProperty(value = "status")
    public LedgerStatus getStatus() {
        return status;
    }

    public void setStatus(LedgerStatus status) {
        this.status = status;
    }

    @JsonIgnore
    public void addEnsemble(Long entryId, List<String> ensemble) {
        ensembles.put(entryId, ensemble);
    }

    @JsonIgnore
    public List<String> getCurrentEnsemble() {
        return ensembles.lastEntry().getValue();
    }

    @JsonIgnore
    public List<String> getEnsembleFor(long entryId) {
        if (entryId == -1L) {
            return getCurrentEnsemble();
        }
        return ensembles.floorEntry(entryId).getValue();
    }

    @JsonIgnore
    public void replaceCurrentEnsemble(List<String> ensemble) {
        ensembles.put(ensembles.lastKey(), ensemble);
    }

    @Override
    public String toString() {
        return "LedgerMetadata{" +
                "ledgerId=" + ledgerId +
                ", writeQuorum=" + writeQuorum +
                ", ackQuorum=" + ackQuorum +
                ", ensembles=" + ensembles +
                ", lastEntryId=" + lastEntryId +
                ", status=" + status +
                '}';
    }
}
