package com.vanlightly.bookkeeper;

public class TransientException extends RuntimeException {
    public TransientException(String message) {
        super(message);
    }

    public TransientException(String message, Throwable cause) {
        super(message, cause);
    }
}
