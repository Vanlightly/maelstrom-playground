package com.vanlightly.bookkeeper.network;

public interface NetworkIO {
    boolean hasNext();
    String readNext();
    void write(String msg);
}
