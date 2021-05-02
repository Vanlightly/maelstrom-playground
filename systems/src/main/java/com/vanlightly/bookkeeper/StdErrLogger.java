package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.vanlightly.bookkeeper.util.Logger;
import com.vanlightly.bookkeeper.kv.bkclient.BkException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class StdErrLogger implements Logger {

    public static final int DEBUG = 0;
    public static final int INFO = 1;
    public static final int ERROR = 2;
    public static int LogLevel = ERROR;
    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private String component;

    public StdErrLogger(String component) {
        this.component = component;
    }

    @Override
    public void logDebug(String text) {
        if (LogLevel == DEBUG) {
            print("DEBUG", text);
            System.err.flush();
        }
    }

    @Override
    public void logInfo(String text) {
        if (LogLevel <= INFO) {
            print("INFO", text);
            System.err.flush();
        }
    }

    @Override
    public void logError(String text) {
        print("ERROR", text);
        System.err.flush();
    }

    @Override
    public void logInvariantViolation(String text, String invCode) {
        print("INVARIANT_VIOLATION", text + " INV_CODE: " + invCode);
        System.err.flush();
    }

    @Override
    public void logError(String text, Throwable t) {
        t = unwrap(t);

        if (t == null) {
            print("ERROR", text);
        } else if (t instanceof BkException) {
            BkException bke = (BkException)t;
            print(" ERROR", text
                    + " Code: " + bke.getCode()
                    + " Message: " + bke.getMessage());
//            bke.printStackTrace(System.err);
        } else if (t instanceof MetadataException) {
            MetadataException me = (MetadataException)t;
            print(" ERROR", text
                    + " Code: " + me.getCode()
                    + " Message: " + me.getMessage());
//            me.printStackTrace(System.err);
        } else if (t instanceof OperationCancelledException){
            print("INFO", "Operation cancelled with message: " + text);
        } else {
            print("ERROR", text);
            t.printStackTrace(System.err);
        }

        System.err.flush();
    }

    @Override
    public void logReplyToTimedOutMsg(JsonNode msg) {
        String command = msg.get(Fields.BODY).get(Fields.MSG_TYPE).asText();
        logInfo("Command " + command + " already timed out - discarding. Msg: " + msg.toString());
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

    private void print(String level, String text) {
        System.err.println(Thread.currentThread().getName()
                + " : " + getTime()
                + " : " + level
                + " : " + component
                + " : " + text);
    }

    private String getTime() {
        LocalDateTime now = LocalDateTime.now();
        return dtf.format(now);
    }
}
