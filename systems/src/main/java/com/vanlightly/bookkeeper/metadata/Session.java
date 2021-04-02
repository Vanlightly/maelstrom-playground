package com.vanlightly.bookkeeper.metadata;

public class Session {
    final String nodeId;
    long renewDeadline;
    final long sessionId;
    boolean valid;

    public Session() {
        valid = false;
        nodeId = "";
        sessionId = -1;
    }

    public Session(String nodeId, long renewDeadline, long sessionId) {
        this.nodeId = nodeId;
        this.renewDeadline = renewDeadline;
        this.sessionId = sessionId;
        this.valid = true;
    }

    public static Session invalidSession() {
        return new Session();
    }

    public boolean isValid() {
        return valid;
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getRenewDeadline() {
        return renewDeadline;
    }

    public void setRenewDeadline(long renewDeadline) {
        this.renewDeadline = renewDeadline;
    }

    public long getSessionId() {
        return sessionId;
    }
}
