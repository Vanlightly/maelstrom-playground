package com.vanlightly.bookkeeper.metadata;

public class Versioned<T> {
    T value;
    long version;

    public Versioned(T value, long version) {
        this.value = value;
        this.version = version;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public void incrementVersion() {
        version++;
    }
}
