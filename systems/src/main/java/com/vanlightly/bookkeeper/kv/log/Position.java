package com.vanlightly.bookkeeper.kv.log;

public class Position {
    private long ledgerId;
    private long entryId;
    private boolean isEndOfLedger;

    public Position(long ledgerId, long entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public Position(long ledgerId, long entryId, boolean isEndOfLedger) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.isEndOfLedger = isEndOfLedger;
    }

    public long getLedgerId() {
        return ledgerId;
    }

    public long getEntryId() {
        return entryId;
    }

    public boolean isEndOfLedger() {
        return isEndOfLedger;
    }

    public void setLedgerId(long ledgerId) {
        this.ledgerId = ledgerId;
    }

    public void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    public void setEndOfLedger(boolean endOfLedger) {
        isEndOfLedger = endOfLedger;
    }

    @Override
    public String toString() {
        return "Position{" +
                "ledgerId=" + ledgerId +
                ", entryId=" + entryId +
                '}';
    }
}
