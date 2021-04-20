package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.utils.NetworkRouter;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class InitializationTests {
    ObjectMapper mapper;
    int msgId;

    public InitializationTests() {
        mapper = new ObjectMapper();
        msgId = 0;
    }

    private void print(JsonNode msg) {
        String dest = msg.get("dest").asText();
        String source = msg.get("src").asText();
        String type = msg.path("body").path("type").asText();

        boolean isReply = msg.path("body").has("in_reply_to");
        String reqOrReply = "request";
        if (isReply) {
            reqOrReply = "reply";
        }

        switch (type) {
            case Commands.Bookie.READ_ENTRY:
                if (isReply) {
                    type = type
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                            + " e=" + msg.path("body").path(Fields.L.ENTRY_ID).asLong()
                            + " lac=" + msg.path("body").path(Fields.L.LAC).asLong();
                } else {
                    type = type
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                            + " e=" + msg.path("body").path(Fields.L.ENTRY_ID).asLong();
                }
                break;
            case Commands.Bookie.READ_LAC:
                if (isReply) {
                    type = type
                            + " lac=" + msg.path("body").path(Fields.L.LAC).asLong();
                } else {
                    type = type
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong();
                }
                break;
            case Commands.Bookie.READ_LAC_LONG_POLL:
                if (isReply) {
                    type = type
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                            + " lac=" + msg.path("body").path(Fields.L.LAC).asLong();
                } else {
                    type = type
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                            + " plac=" + msg.path("body").path(Fields.L.PREVIOUS_LAC).asLong();
                }
                break;
            case Commands.Bookie.ADD_ENTRY:
                type = type
                        + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                        + " e=" + msg.path("body").path(Fields.L.ENTRY_ID).asLong()
                        + " lac=" + msg.path("body").path(Fields.L.LAC).asLong();
                break;
        }

        System.out.println("(" + source + ")-[" + type + "]->(" + dest + ") " + reqOrReply);
    }

    @Test
    public void testNoKvRequests() {
        AtomicBoolean isCancelled = new AtomicBoolean();
        NetworkRouter router = new NetworkRouter(isCancelled,
                (msg) -> print(msg));
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
        NetworkRouter router = new NetworkRouter(isCancelled,
                (msg) -> print(msg));
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
            List<String> partionable = Arrays.asList("n5", "n6");
            Random r  = new Random();
            String lastNode = null;

            for (int i=0; i<100; i++) {
                String kvStoreNode = Node.getKvStoreNode();
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
                if (i %10 == 0) {
                    String target = partionable.get(r.nextInt(partionable.size()));
                    router.partitionNode(target, true, true);
                    if (lastNode != null && !lastNode.equals(target)) {
                        router.healNode(target);
                    }
                    lastNode = target;
                    Thread.sleep(2000);
                }
            }

            Thread.sleep(100000000);
        } catch (Exception e) {

        }
    }
}
