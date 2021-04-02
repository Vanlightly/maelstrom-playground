package com.vanlightly.bookkeeper.metadata;

public class Expirable<T> {
    T value;
    long renewDeadline;

    public Expirable(T value, long renewDeadline) {
        this.value = value;
        this.renewDeadline = renewDeadline;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getRenewDeadline() {
        return renewDeadline;
    }

    public void setRenewDeadline(long renewDeadline) {
        this.renewDeadline = renewDeadline;
    }

    public boolean hasExpired() {
        return System.nanoTime() < renewDeadline;
    }
}
