package com.vanlightly.bookkeeper.util;

public class LogManager {
    public static Logger getLogger(String component) {
        return new StdErrLogger(component);
    }
}
