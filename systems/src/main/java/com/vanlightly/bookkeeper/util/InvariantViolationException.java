package com.vanlightly.bookkeeper.util;

public class InvariantViolationException extends RuntimeException {
    public InvariantViolationException(String message) {
        super(message);
    }
}
