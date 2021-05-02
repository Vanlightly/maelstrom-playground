package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.network.NetworkIO;
import com.vanlightly.bookkeeper.network.StdInOutNetwork;

public class NodeRunner {

    public static void main(String[] args) {
        StdErrLogger.LogLevel = StdErrLogger.DEBUG;
        NodeRunner nodeRunner = new NodeRunner();
        nodeRunner.run();
    }

    private NetworkIO net;
    private ObjectMapper mapper;
    private Node node;

    public NodeRunner() {
        this.net = new StdInOutNetwork();
        this.mapper = new ObjectMapper();
    }

    public NodeRunner(NetworkIO net) {
        this.net = net;
        this.mapper = new ObjectMapper();
    }

    public void run() {
        System.err.println("Waiting for initialization");

        try {
            node = waitForInitMsg();
            System.err.println("Initialization begun");

            while(true) {
                boolean actionTaken = nextAction();
                if (!actionTaken) {
                    Thread.sleep(10);
                }
            }
        } catch(Throwable t) {
            System.err.println("Node " + node.nodeId + " exiting due to error:");
            t.printStackTrace(System.err);
        }
    }

    private boolean nextAction() throws JsonProcessingException {
        return node.handleTimeout()
                || node.resumeDelayedTask()
                || node.roleSpecificAction()
                || handleIncomingMsg();
    }

    private boolean handleIncomingMsg() throws JsonProcessingException {
        if (net.hasNext()) {
            String input = net.readNext();
            JsonNode msg = mapper.readTree(input);

            if (msg.get(Fields.BODY).has(Fields.IN_REPLY_TO)) {
                node.handleReply(msg);
            } else {
                node.handleRequest(msg);
            }

            return true;
        } else {
            return false;
        }
    }

    private Node waitForInitMsg() throws JsonProcessingException {
        while (true) {
            if (net.hasNext()) {
                String input = net.readNext();
                JsonNode msg = mapper.readTree(input);

                String type = msg.get(Fields.BODY).get(Fields.MSG_TYPE).asText();

                if (type.equals("init")) {
                    Node node = buildNode(msg, net, mapper);
                    node.initialize(msg);
                    return node;
                } else {
                    throw new RuntimeException("Expect first message to be init. Instead received: " + type);
                }
            } else {
                try {
                    Thread.sleep(1000);
                } catch(InterruptedException e) {}
            }
        }
    }

    private Node buildNode(JsonNode msg, NetworkIO net, ObjectMapper mapper) {
        String nodeId = msg.get(Fields.BODY).get("node_id").asText();
        Logger logger = new StdErrLogger();
        NodeType nodeType = Node.determineType(nodeId);

        Node node;

        if (nodeType == NodeType.MetadataStore) {
            node = new MetadataStoreNode(String.valueOf(nodeId), net, logger, mapper, null);
            System.err.println("Built metadata store node " + nodeId);
        } else {
            ManagerBuilder builder = new ManagerBuilderImpl(mapper, logger);

            if (nodeType == NodeType.KvStore) {
                node = new KvStoreNode(nodeId, net, logger, mapper, builder);
                System.err.println("Built bk client node " + nodeId);
            } else {
                node = new BookieNode(nodeId, net, logger, mapper, builder);
                System.err.println("Built bookie node " + nodeId);
            }
        }
        return node;
    }
}
