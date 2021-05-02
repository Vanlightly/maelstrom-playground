package com.vanlightly.bookkeeper.util;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MsgMapping {
    public static ObjectMapper getMapper() {
        return new ObjectMapper();
    }
}
