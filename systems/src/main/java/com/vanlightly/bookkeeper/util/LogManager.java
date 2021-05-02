package com.vanlightly.bookkeeper.util;

import com.vanlightly.bookkeeper.StdErrLogger;

public class LogManager {
    public static Logger getLogger(String component) {
        return new StdErrLogger(component);
    }
}
