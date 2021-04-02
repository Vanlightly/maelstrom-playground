package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;

public interface Logger {
    void logDebug(String text);
    void logInfo(String text);
    void logError(String text);
    void logError(String text, Throwable e);
    void logBadReturnCode(String rc, String command, JsonNode msg);
    void logStaleMsg(JsonNode msg);
    void logBadSession(JsonNode msg, long msgSessionId, long realSessionId);
}
