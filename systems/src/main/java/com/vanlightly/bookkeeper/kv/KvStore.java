package com.vanlightly.bookkeeper.kv;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.Constants;
import com.vanlightly.bookkeeper.Fields;
import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;

import java.util.HashMap;
import java.util.Map;

public class KvStore {
    private Logger logger = LogManager.getLogger(this.getClass().getName());
    Map<String, Integer> store;
    ObjectMapper mapper;

    public KvStore(ObjectMapper mapper) {
        this.mapper = mapper;
        this.store = new HashMap<>();
    }

    public ObjectNode apply(Map<String, String> op) {
        ObjectNode body = mapper.createObjectNode();

        String key = op.get(Fields.KV.Op.KEY);
        String type = op.get(Fields.KV.Op.TYPE);
        body.put(Fields.IN_REPLY_TO, Integer.parseInt(op.get(Fields.MSG_ID)));

        switch (type) {
            case Constants.KvStore.Ops.WRITE:
                store.put(key, Integer.parseInt(op.get(Fields.KV.Op.VALUE)));
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
                    int currVal = store.get(key);
                    int fromVal = Integer.parseInt(op.get(Fields.KV.Op.FROM));
                    int toVal = Integer.parseInt(op.get(Fields.KV.Op.TO));

                    if (currVal == fromVal) {
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

        logger.logDebug("Apply. Op: " + op + " Body: " + body);

        return body;
    }

    public void printState() {
        logger.logInfo("----------------- KV State -------------");
        logger.logDebug(store.toString());
        logger.logInfo("----------------------------------------");
    }

}
