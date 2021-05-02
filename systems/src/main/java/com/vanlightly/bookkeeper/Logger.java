package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;

public interface Logger {
    void logDebug(String text);
    void logInfo(String text);
    void logError(String text);
    void logError(String text, Throwable e);
    void logReplyToTimedOutMsg(JsonNode msg);
    void logBadSession(JsonNode msg, long msgSessionId, long realSessionId);
    void logInvariantViolation(String text, String invCode);
}
