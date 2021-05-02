package com.vanlightly.bookkeeper.kv.log;

import java.util.Objects;

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

    public Position(Position pos) {
        this.ledgerId = pos.getLedgerId();
        this.entryId = pos.getEntryId();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Position position = (Position) o;
        return getLedgerId() == position.getLedgerId() &&
                getEntryId() == position.getEntryId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(getLedgerId(), getEntryId());
    }
}
