package com.vanlightly.bookkeeper.kv.bkclient;

public class BkException extends RuntimeException {
    private String code;

    public BkException(String message, String code) {
        super(message);
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
