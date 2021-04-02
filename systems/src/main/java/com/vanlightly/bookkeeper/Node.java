package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.network.NetworkIO;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Node implements MessageSender {
    public final static String MetadataNodeId = "n1";

    protected NetworkIO net;
    protected Logger logger;
    protected SessionManager sessionManager;
    protected ObjectMapper mapper;
    protected String nodeId;
    protected int nextMsgId;
    protected AtomicBoolean isCancelled;

    private NavigableMap<Long, JsonNode> pendingReplyDeadlines;
    private Queue<JsonNode> timedOutCommands;
    private final int timeoutMs;
    private Map<Integer, CompletableFuture<JsonNode>> replyCallbacks;

    public Node(String nodeId,
                boolean useKeepAlives,
                NetworkIO net,
                Logger logger,
                ObjectMapper mapper,
                ManagerBuilder builder) {
        this.nodeId = nodeId;
        this.net = net;
        this.mapper = mapper;
        this.logger = logger;
        this.isCancelled = new AtomicBoolean();

        if (builder != null) {
            if (useKeepAlives) {
                this.sessionManager = builder.buildSessionManagerWithKeepAlives(
                        Constants.KeepAlives.KeepAliveIntervalMs, this);
            } else {
                this.sessionManager = builder.buildSessionManagerWithoutKeepAlives(this);
            }
        }

        this.nextMsgId = 1;
        this.pendingReplyDeadlines = new TreeMap<>();
        this.timeoutMs = Constants.Timeouts.TimeoutMs;
        this.timedOutCommands = new ArrayDeque<>();
        this.replyCallbacks = new HashMap<>();
    }

    public static NodeType determineType(String nodeId) {
        int nodeOrdinal = Integer.valueOf(nodeId.replace("n", ""));
        if (nodeOrdinal == 1) {
            return NodeType.MetadataStore;
        } else if (nodeOrdinal <= 4) {
            return NodeType.Bookie;
        } else {
            return NodeType.KvStore;
        }
    }

    public Logger getLogger() {
        return logger;
    }

    public void checkForTimeouts() {
        try {
            SortedMap<Long, JsonNode> timedOut = pendingReplyDeadlines.headMap(System.currentTimeMillis());
            for (Map.Entry<Long, JsonNode> timedOutMsg : timedOut.entrySet()) {
                ObjectNode msg = mapper.createObjectNode();

                ObjectNode body = mapper.createObjectNode();
                body.put(Fields.MSG_TYPE, timedOutMsg.getValue().get(Fields.BODY).get(Fields.MSG_TYPE).asText());
                body.put(Fields.RC, ReturnCodes.TIME_OUT);

                body.set(Fields.BODY, body);
                msg.put("in_reply_to", timedOutMsg.getValue().get(Fields.MSG_ID).asText());
                msg.put(Fields.SOURCE, timedOutMsg.getValue().get(Fields.DEST).asText());
                msg.put(Fields.DEST, nodeId);

                timedOutCommands.add(msg);
            }
        } catch (Exception e) {
            //logger.logError("Failed checking for timeouts", e);
        }
    }

    public boolean hasTimeOuts() {
        return !timedOutCommands.isEmpty();
    }

    public JsonNode getTimedOutMsg() {
        return timedOutCommands.poll();
    }

    public void send(String destId, String command) {
        ObjectNode body = mapper.createObjectNode();
        sendRequest(destId, command, body, null);
    }

    public void send(String destId, String command, ObjectNode body) {
        sendRequest(destId, command, body, null);
    }

    public CompletableFuture<JsonNode> sendRequest(String destId, String command) {
        ObjectNode body = mapper.createObjectNode();
        return sendRequest(destId, command, body);
    }

    public CompletableFuture<JsonNode> sendRequest(String destId, String command, ObjectNode body) {
        CompletableFuture<JsonNode> replyFuture = new CompletableFuture<>();
        sendRequest(destId, command, body, replyFuture);
        return replyFuture;
    }

    private void sendRequest(String destId, String command, ObjectNode body, CompletableFuture<JsonNode> replyFuture) {
        nextMsgId++;

        if (body == null) {
            body = mapper.createObjectNode();
        }

        body.put(Fields.MSG_TYPE, command);
        body.put(Fields.MSG_ID, nextMsgId);

        ObjectNode msg = mapper.createObjectNode();
        msg.put(Fields.SOURCE, nodeId);
        msg.put(Fields.DEST, destId);
        msg.set(Fields.BODY, body);

        if (replyFuture != null) {
            replyCallbacks.put(nextMsgId, replyFuture);
            pendingReplyDeadlines.put(System.currentTimeMillis() + timeoutMs, msg);
        }

        net.write(msg.toString());
    }

    public void reply(JsonNode msg, String returnCode) {
        ObjectNode body = mapper.createObjectNode();
        reply(msg, returnCode, body);
    }

    public void reply(JsonNode msg, String returnCode, JsonNode replyBody) {
        nextMsgId++;
        ObjectNode body = (ObjectNode)replyBody;

        if (returnCode != null) {
            body.put(Fields.RC, returnCode);
        }
        body.put(Fields.MSG_ID, nextMsgId);
        body.set("in_reply_to", msg.get(Fields.BODY).get(Fields.MSG_ID));

        ObjectNode outMsg = mapper.createObjectNode();
        outMsg.put(Fields.SOURCE, nodeId);
        outMsg.put(Fields.DEST, msg.get(Fields.SOURCE).asText());
        outMsg.set(Fields.BODY, body);

        net.write(outMsg.toString());
    }

    public boolean handleTimeout() {
        if (hasTimeOuts()) {
            handleReply(getTimedOutMsg());
            return true;
        } else {
            return false;
        }
    }

    public void handleReply(JsonNode reply) {
        logger.logDebug("Received reply: " + reply.toString());
        int msgId = reply.get(Fields.BODY).get(Fields.IN_REPLY_TO).asInt();
        CompletableFuture<JsonNode> replyCallback = replyCallbacks.get(msgId);

        if (replyCallback != null) {
            replyCallback.complete(reply);
        } else {
            logger.logStaleMsg(reply);
        }
    }

    protected void sendInitOk(JsonNode initMsg) {
        ObjectNode replyBody = mapper.createObjectNode();
        replyBody.put(Fields.MSG_TYPE, "init_ok");
        reply(initMsg, null, replyBody);
    }

    abstract void initialize(JsonNode initMsg);
    abstract boolean roleSpecificAction();
    abstract void handleRequest(JsonNode request);
}
