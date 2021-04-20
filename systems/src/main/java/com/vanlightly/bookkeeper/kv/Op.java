package com.vanlightly.bookkeeper.kv;

import com.fasterxml.jackson.databind.exc.InvalidFormatException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Op {

    private long opId;
    private Map<String,String> fields;
    private boolean committed;

    public Op(long opId) {
        this.opId = opId;
    }

    public Op(long opId, Map<String, String> fields) {
        this.opId = opId;
        this.fields = fields;
    }

    public long getOpId() {
        return opId;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Op op = (Op) o;
        return op.opId == this.opId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getFields(), isCommitted());
    }

    public static Op stringToOp(String opStr) {
        Map<String,String> opFields = new HashMap<>();
        String[] opParts = opStr.split("@");
        long id = Long.parseLong(opParts[0]);

        for (String keyVal : opParts[1].split(",")) {
            String[] kv = keyVal.split("=");
            if (kv.length != 2) {
                throw new IllegalArgumentException("Bad Op format: " + opStr);
            } else {
                opFields.put(kv[0], kv[1]);
            }
        }

        return new Op(id, opFields);
    }

    public static String opToString(Op op) {
        return op.opId + "@" + String.join(",",
                op.getFields().entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.toList()));
    }
}
