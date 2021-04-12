package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.utils.NetworkRouter;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class InitializationTests {
    ObjectMapper mapper;
    int msgId;

    public InitializationTests() {
        mapper = new ObjectMapper();
        msgId = 0;
    }

    @Test
    public void testNoKvRequests() {
        AtomicBoolean isCancelled = new AtomicBoolean();
        NetworkRouter router = new NetworkRouter(isCancelled);
        int nodeCount = 6;

        for (int n = 1; n <= nodeCount; n++) {
            String nodeId = "n" + n;
            router.addNode(nodeId);
            String init = "{\"dest\":\"" + nodeId +"\",\"body\":{\"type\":\"init\",\"node_id\":\"" + nodeId +"\",\"msg_id\":" + msgId + "},\"src\":\"c1\"}\n";
            router.send(nodeId, init);
            msgId++;
        }

        router.routeMessages();

        try {
            Thread.sleep(100000000);
        } catch (Exception e) {

        }
    }

    @Test
    public void testOneKvWrite() {
        AtomicBoolean isCancelled = new AtomicBoolean();
        NetworkRouter router = new NetworkRouter(isCancelled);
        int nodeCount = 6;

        for (int n = 1; n <= nodeCount; n++) {
            String nodeId = "n" + n;
            router.addNode(nodeId);
            String init = "{\"dest\":\"" + nodeId +"\",\"body\":{\"type\":\"init\",\"node_id\":\"" + nodeId +"\",\"msg_id\":" + msgId + "},\"src\":\"c1\"}\n";
            router.send(nodeId, init);
            msgId++;
        }

        router.routeMessages();

        try {
            Thread.sleep(2000);

            for (int i=0; i<1000; i++) {

                String kvStoreNode = Node.getFirstKvStoreNodeId();
                ObjectNode op = mapper.createObjectNode();
                op.put(Fields.KV.Op.TYPE, Constants.KvStore.Ops.WRITE);
                op.put(Fields.KV.Op.KEY, "A"+i);
                op.put(Fields.KV.Op.VALUE, "B"+i);

                ObjectNode body = mapper.createObjectNode();
                body.put(Fields.MSG_TYPE, Constants.KvStore.Ops.WRITE);
                body.put(Fields.MSG_ID, msgId);
                body.set(Fields.KV.OP, op);

                ObjectNode writeMsg = mapper.createObjectNode();
                writeMsg.put(Fields.DEST, kvStoreNode);
                writeMsg.put(Fields.SOURCE, "c1");
                writeMsg.set(Fields.BODY, body);

                msgId++;
                router.send(kvStoreNode, writeMsg.toString());
                if (i %100 == 0) {
                    Thread.sleep(2000);
                }
            }

            Thread.sleep(100000000);
        } catch (Exception e) {

        }
    }
}
