package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.utils.NetworkRouter;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterRunner {
    public static void main(String[] args) {
        int bookieCount = Integer.valueOf(args[0]);
        int kvStoreCount = Integer.valueOf(args[1]);
        ClusterRunner cr = new ClusterRunner(bookieCount, kvStoreCount);
        cr.initialize();

        Scanner scanner = new Scanner(System.in);
        while (true) {
            String input = scanner.nextLine();
            String[] parts = input.split(" ");
            String command = parts[0];
            switch (command) {
                case "w":
                    String key = parts[1];
                    String value = parts[2];
                    cr.write(key, value);
                    break;
                case "r":
                    key = parts[1];
                    cr.read(key);
                    break;
                case "c":
                    key = parts[1];
                    String from = parts[2];
                    String to = parts[3];
                    cr.cas(key, from, to);
                    break;
                case "s":
                    String nodeId = parts[1];
                    cr.printState(nodeId);
                    break;
                case "p":
                    if (parts.length == 2) {
                        nodeId = parts[1];
                        cr.partitionNode(nodeId);
                    } else if (parts.length == 3) {
                        String node1 = parts[1];
                        String node2 = parts[2];
                        cr.partitionNodes(node1, node2);
                    }
                    break;
                case "h":
                    if (parts.length == 2) {
                        nodeId = parts[1];
                        cr.healNode(nodeId);
                    } else if (parts.length == 3) {
                        String node1 = parts[1];
                        String node2 = parts[2];
                        cr.healNodes(node1, node2);
                    }
                    break;
                default:
                    System.out.println("Bad command");
            }
        }
    }

    int nodeCount;
    int bookies;
    int kvStores;
    ObjectMapper mapper;
    int msgId;
    AtomicBoolean isCancelled;
    NetworkRouter router;
    HashSet<Integer> doNotPrintMsgIds;

    public ClusterRunner(int bookies, int kvStores) {
        this.nodeCount = bookies + kvStores + 1;
        this.bookies = bookies;
        this.kvStores = kvStores;
        this.mapper = new ObjectMapper();
        this.msgId = 0;
        this.isCancelled = new AtomicBoolean();
        this.router = new NetworkRouter(isCancelled, (msg) -> print(msg));
        this.doNotPrintMsgIds = new HashSet<>();
    }

    public void initialize() {
        StdErrLogger.LogLevel = StdErrLogger.DEBUG;
        Constants.Bookie.BookieCount = bookies;
        for (int n = 0; n < nodeCount; n++) {
            String nodeId = "n" + n;
            router.addNode(nodeId);
            String init = "{\"dest\":\"" + nodeId + "\",\"body\":{\"type\":\"init\",\"node_id\":\"" + nodeId + "\",\"msg_id\":" + msgId + "},\"src\":\"c1\"}\n";
            router.send(nodeId, init);
            msgId++;
        }

        router.routeMessages();
    }

    public void printState(String nodeId) {
        ObjectNode body = mapper.createObjectNode();
        body.put(Fields.MSG_TYPE, Commands.PRINT_STATE);
        body.put(Fields.MSG_ID, msgId);

        ObjectNode msg = mapper.createObjectNode();
        msg.put(Fields.DEST, nodeId);
        msg.put(Fields.SOURCE, "c1");
        msg.set(Fields.BODY, body);

        msgId++;
        router.send(nodeId, msg.toString());
    }

    public void write(String key, String value) {
        String kvStoreNode = getKvStoreNode();
        if (kvStoreNode == null) {
            System.out.println("Write dropped. All KV Store nodes are partitioned");
        }

        ObjectNode body = mapper.createObjectNode();
        body.put(Fields.MSG_TYPE, Constants.KvStore.Ops.WRITE);
        body.put(Fields.MSG_ID, msgId);
        body.put(Fields.KV.Op.KEY, key);
        body.put(Fields.KV.Op.VALUE, value);

        ObjectNode writeMsg = mapper.createObjectNode();
        writeMsg.put(Fields.DEST, kvStoreNode);
        writeMsg.put(Fields.SOURCE, "c1");
        writeMsg.set(Fields.BODY, body);

        msgId++;
        router.send(kvStoreNode, writeMsg.toString());
    }

    public void read(String key) {
        String kvStoreNode = getKvStoreNode();
        if (kvStoreNode == null) {
            System.out.println("Read dropped. All KV Store nodes are partitioned");
        }

        ObjectNode op = mapper.createObjectNode();
        op.put(Fields.KV.Op.TYPE, Commands.Client.READ);

        ObjectNode body = mapper.createObjectNode();
        body.put(Fields.MSG_TYPE, Constants.KvStore.Ops.READ);
        body.put(Fields.MSG_ID, msgId);
        body.put(Fields.KV.Op.KEY, key);

        ObjectNode writeMsg = mapper.createObjectNode();
        writeMsg.put(Fields.DEST, kvStoreNode);
        writeMsg.put(Fields.SOURCE, "c1");
        writeMsg.set(Fields.BODY, body);

        msgId++;
        router.send(kvStoreNode, writeMsg.toString());
    }

    public void cas(String key, String from, String to) {
        String kvStoreNode = getKvStoreNode();
        if (kvStoreNode == null) {
            System.out.println("CAS dropped. All KV Store nodes are partitioned");
        }

        ObjectNode body = mapper.createObjectNode();
        body.put(Fields.MSG_TYPE, Constants.KvStore.Ops.CAS);
        body.put(Fields.MSG_ID, msgId);
        body.put(Fields.KV.Op.KEY, key);
        body.put(Fields.KV.Op.FROM, from);
        body.put(Fields.KV.Op.TO, to);

        ObjectNode writeMsg = mapper.createObjectNode();
        writeMsg.put(Fields.DEST, kvStoreNode);
        writeMsg.put(Fields.SOURCE, "c1");
        writeMsg.set(Fields.BODY, body);

        msgId++;
        router.send(kvStoreNode, writeMsg.toString());
    }

    public void partitionNode(String nodeId) {
        router.partitionNode(nodeId);
    }

    public void partitionNodes(String node1, String node2) {
        router.partitionNodes(node1, node2);
    }

    public void healNode(String nodeId) {
        router.healNode(nodeId);
    }

    public void healNodes(String node1, String node2) {
        router.healNodes(node1, node2);
    }

    private String getKvStoreNode() {
        for (int k=1; k <= kvStores; k++) {
            String nodeId = "n" + (bookies + 1 + k);
            if (!router.isPartitioned(nodeId)) {
                return nodeId;
            }
        }

        return null;
    }

    private void print(JsonNode msg) {
        String dest = msg.get(Fields.DEST).asText();
        String source = msg.get(Fields.SOURCE).asText();
        String msgId = msg.path(Fields.BODY).path(Fields.MSG_ID).asText();
        String inReplyToMsgId = "";
        String type = msg.path(Fields.BODY).path(Fields.MSG_TYPE).asText();

        if (type.equals(Commands.Metadata.SESSION_KEEP_ALIVE)
            || type.equals(Commands.Metadata.GET_LEADER_ID)) {
            return;
        }

        String rep = type;

        boolean isReply = msg.path("body").has("in_reply_to");
        String reqOrReply = "request";
        if (isReply) {
            reqOrReply = "reply";
            inReplyToMsgId = "/" + msg.get(Fields.BODY).get(Fields.IN_REPLY_TO).asText();
//            int msgId = msg.get(Fields.BODY).get(Fields.IN_REPLY_TO).asInt();
//
//            if (doNotPrintMsgIds.contains(msgId)) {
//                return;
//            }
        }

        switch (type) {
            case Commands.Client.CAS_OK:
                rep = rep + " ## CAS OK ##";
                break;
            case Commands.Client.WRITE_OK:
                rep = rep + " ## WRITE OK ##";
                break;
            case Commands.Client.READ_OK:
                JsonNode body = msg.get("body");
                rep = rep + " ## READ OK ## "
                        + " value=" + body.get(Fields.KV.Op.VALUE);
                break;
            case Commands.Client.ERROR:
                body = msg.get("body");
                rep = rep + " ## CLIENT CMD ERROR ## "
                        + " type=" + body.get(Fields.KV.Op.CMD_TYPE)
                        + " code=" + body.get(Fields.KV.Op.CODE)
                        + " text=" + body.get(Fields.KV.Op.ERROR_TEXT);
                break;
            case Commands.Bookie.READ_ENTRY:
                if (isReply) {
                    rep = rep
                            + " rc=" + msg.path("body").path(Fields.RC).asText()
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                            + " e=" + msg.path("body").path(Fields.L.ENTRY_ID).asLong()
                            + " lac=" + msg.path("body").path(Fields.L.LAC).asLong();
                } else {
                    rep = rep
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                            + " e=" + msg.path("body").path(Fields.L.ENTRY_ID).asLong();
                }
                break;
            case Commands.Bookie.READ_LAC:
                if (isReply) {
                    rep = rep
                            + " rc=" + msg.path("body").path(Fields.RC).asText()
                            + " lac=" + msg.path("body").path(Fields.L.LAC).asLong();
                } else {
                    rep = rep
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong();
                }
                break;
            case Commands.Bookie.READ_LAC_LONG_POLL:
                if (isReply) {
                    rep = rep
                            + " rc=" + msg.path("body").path(Fields.RC).asText()
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                            + " entryId=" + msg.path("body").path(Fields.L.ENTRY_ID).asLong()
                            + " lac=" + msg.path("body").path(Fields.L.LAC).asLong()
                            + " value=" + msg.path("body").path(Fields.L.VALUE).asText("null");
                } else {
                    rep = rep
                            + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                            + " plac=" + msg.path("body").path(Fields.L.PREVIOUS_LAC).asLong();
                }
                break;
            case Commands.Bookie.ADD_ENTRY:
                String opStr = msg.path("body").path(Fields.L.VALUE).asText();

                if (isReply) {
                    rep = rep + " rc=" + msg.path("body").path(Fields.RC).asText();
                }
                rep = rep
                        + " l=" + msg.path("body").path(Fields.L.LEDGER_ID).asLong()
                        + " e=" + msg.path("body").path(Fields.L.ENTRY_ID).asLong()
                        + " lac=" + msg.path("body").path(Fields.L.LAC).asLong()
                        + " op=" + opStr;
                break;
            case Commands.Metadata.SESSION_KEEP_ALIVE:
                rep = rep + " session=" + msg.path("body").path(Fields.SESSION_ID).asText();

                if (isReply) {
                    rep = rep + " rc=" + msg.path("body").path(Fields.RC).asText();
                }

                break;
        }

        System.out.println("(" + source + ")-[" + rep
                + "{" + msgId + inReplyToMsgId + "}]->(" + dest + ") " + reqOrReply);
    }
}
