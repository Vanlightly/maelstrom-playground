package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.vanlightly.bookkeeper.Logger;

public class StdErrLogger implements Logger {

    @Override
    public void logDebug(String text) {
        System.err.println(Thread.currentThread().getName() + " DEBUG: " + text);
        System.err.flush();
    }

    @Override
    public void logInfo(String text) {
        System.err.println(Thread.currentThread().getName() + " INFO: " + text);
        System.err.flush();
    }

    @Override
    public void logError(String text) {
        System.err.println(Thread.currentThread().getName() + " ERROR: " + text);
        System.err.flush();
    }

    @Override
    public void logError(String text, Throwable e) {
        System.err.println(Thread.currentThread().getName() + " ERROR: " + text);
        e.printStackTrace(System.err);
        System.err.flush();
    }

    @Override
    public void logBadReturnCode(String rc, String command, JsonNode msg) {
        logError("Unexpected return code " + rc + " in reply to "
                + command + " " + " in message: " + msg.toString());
    }

    @Override
    public void logStaleMsg(JsonNode msg) {
        String command = msg.get(Fields.BODY).get(Fields.MSG_TYPE).asText();
        logInfo("Ignoring " + command + " command. Either is stale or bad transition. Msg: " + msg.toString());
    }

    @Override
    public void logBadSession(JsonNode msg, long msgSessionId, long realSessionId) {
        String command = msg.get(Fields.BODY).get(Fields.MSG_TYPE).asText();
        logInfo("Ignoring " + command + " command. Expected session: "
                + realSessionId + " but received: " + msgSessionId
                + ". Msg: " + msg.toString());
    }
}
