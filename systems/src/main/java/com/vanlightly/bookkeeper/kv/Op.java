package com.vanlightly.bookkeeper.kv;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class Op {

    private Map<String,String> fields;
    private boolean committed;

    public Op() {
    }

    public Op(Map<String, String> fields) {
        this.fields = fields;
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

    public static Op stringToOp(String opStr) {
        Map<String,String> opFields = new HashMap<>();

        for (String keyVal : opStr.split("|")) {
            String[] parts = keyVal.split("=");
            opFields.put(parts[0], parts[1]);
        }

        return new Op(opFields);
    }

    public static String opToString(Op op) {
        return String.join(",",
                op.getFields().entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.toList()));
    }
}
