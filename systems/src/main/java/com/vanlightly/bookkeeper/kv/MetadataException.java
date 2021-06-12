package com.vanlightly.bookkeeper.kv;

public class MetadataException extends RuntimeException {
    String code;

    public MetadataException(String message, String code) {
        super(message);
        this.code = code;
    }

    public String getCode() {
        return code;
    }
}
