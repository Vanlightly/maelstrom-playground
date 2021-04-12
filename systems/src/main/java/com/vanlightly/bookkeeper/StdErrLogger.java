package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.vanlightly.bookkeeper.Logger;
import com.vanlightly.bookkeeper.kv.bkclient.BkException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

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
    public void logError(String text, Throwable t) {
        if (t instanceof BkException) {
            BkException bke = (BkException)t;
            System.err.println(Thread.currentThread().getName() + " ERROR: " + text
                    + " Code: " + bke.getCode()
                    + " Message: " + bke.getMessage());
            bke.printStackTrace(System.err);
        } else {
            System.err.println(Thread.currentThread().getName() + " ERROR: " + text);
            unwrap(t).printStackTrace(System.err);
        }

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

    private static Throwable unwrap(Throwable t) {
        if (t instanceof ExecutionException || t instanceof CompletionException) {
            return unwrap(t.getCause());
        } else {
            return t;
        }
    }
}
