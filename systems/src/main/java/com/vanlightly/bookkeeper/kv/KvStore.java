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

        String key = op.get(Fields.KV.Op.KEY);
        String type = op.get(Fields.KV.Op.TYPE);
        body.put(Fields.KV.Op.CMD_TYPE, type);

        switch (type) {
            case Constants.KvStore.Ops.WRITE:
                store.put(key, op.get(Fields.KV.Op.VALUE));
                body.put(Fields.KV.Op.TYPE, "write_ok");
                break;
            case Constants.KvStore.Ops.READ:
                if (store.containsKey(key)) {
                    body.put(Fields.KV.Op.TYPE, "read_ok");
                    body.put(Fields.KV.Op.VALUE, store.get(key));
                } else {
                    body.put(Fields.KV.Op.TYPE, "error");
                    body.put(Fields.KV.Op.CODE, 20);
                    body.put(Fields.KV.Op.ERROR_TEXT, "not found");
                }
                break;
            case Constants.KvStore.Ops.CAS:
                if (store.containsKey(key)) {
                    String currVal = store.get(key);
                    String fromVal = op.get(Fields.KV.Op.FROM);
                    String toVal = op.get(Fields.KV.Op.TO);

                    if (currVal.equals(fromVal)) {
                        body.put(Fields.KV.Op.TYPE, "cas_ok");
                        store.put(key, toVal);
                    } else {
                        body.put(Fields.KV.Op.TYPE, "error");
                        body.put(Fields.KV.Op.CODE, 22);
                        body.put(Fields.KV.Op.ERROR_TEXT, "expected " + fromVal + " but had " + currVal);
                    }
                } else {
                    body.put(Fields.KV.Op.TYPE, "error");
                    body.put(Fields.KV.Op.CODE, 20);
                    body.put(Fields.KV.Op.ERROR_TEXT, "not found");
                }
                break;
            default:
                throw new RuntimeException("Operation not supported");
        }

        logger.logDebug("Apply result: " + body + " KV: " + store);

        return body;
    }

    public void printState() {
        logger.logInfo("----------------- KV State -------------");
        logger.logDebug(store.toString());
        logger.logInfo("----------------------------------------");
    }

}
