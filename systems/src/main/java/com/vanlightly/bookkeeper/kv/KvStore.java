package com.vanlightly.bookkeeper.kv;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.Constants;
import com.vanlightly.bookkeeper.Fields;
import com.vanlightly.bookkeeper.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class KvStore {
    Logger logger;
    Map<String, String> store;
    ObjectMapper mapper;

    public KvStore(ObjectMapper mapper, Logger logger) {
        this.mapper = mapper;
        this.logger = logger;
        this.store = new HashMap<>();
    }

    public ObjectNode apply(Map<String, String> op) {
        ObjectNode body = mapper.createObjectNode();

        String key = op.get("key");
        switch (op.get("type")) {
            case Constants.KvStore.Ops.WRITE:
                store.put(key, op.get("value"));
                body.put("type", "write_ok");
                break;
            case Constants.KvStore.Ops.READ:
                if (store.containsKey(key)) {
                    body.put("type", "read_ok");
                    body.put("value", store.get(key));
                } else {
                    body.put("type", "error");
                    body.put("code", 20);
                    body.put("text", "not found");
                }
                break;
            case Constants.KvStore.Ops.CAS:
                if (store.containsKey(key)) {
                    String currVal = store.get(key);
                    String fromVal = op.get("from");
                    String toVal = op.get("to");

                    if (currVal.equals(fromVal)) {
                        body.put("type", "cas_ok");
                        store.put(key, toVal);
                    } else {
                        body.put("type", "error");
                        body.put("code", 22);
                        body.put("text", "expected " + fromVal + " but had " + currVal);
                    }
                } else {
                    body.put("type", "error");
                    body.put("code", 20);
                    body.put("text", "not found");
                }
            default:
                throw new RuntimeException("Operation not supported");
        }

        logger.logDebug("Apply result: " + body + " KV: " + store);

        return body;
    }


}
