package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.network.NetworkIO;
import com.vanlightly.bookkeeper.util.DeadlineCollection;
import com.vanlightly.bookkeeper.util.Futures;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/*
    The node base class contains the logic for sending messages
    and handling replies, as well as some auxiliary tasks such
    as timeout handling and scheduling delayed tasks.

    A node is single threaded and so things like timeouts and delays
    are linearized with the external triggers of receiving messages.
 */
public abstract class Node implements MessageSender {
    public final static String MetadataNodeId = "n1";
    public static int BookieCount = 3; // default is 3 but is configurable.

    protected NetworkIO net;
    protected Logger logger;
    protected SessionManager sessionManager;
    protected ManagerBuilder builder;
    protected ObjectMapper mapper;
    protected String nodeId;
    protected int nextMsgId;
    protected AtomicBoolean isCancelled;

    private Map<Integer, CompletableFuture<JsonNode>> replyCallbacks;
    private DeadlineCollection<JsonNode> replyDeadlines;
    private DeadlineCollection<CompletableFuture<Void>> delayedFutures;

    public Node(String nodeId,
                boolean includeSessionManagement,
                NetworkIO net,
                Logger logger,
                ObjectMapper mapper,
                ManagerBuilder builder) {
        this.nodeId = nodeId;
        this.net = net;
        this.mapper = mapper;
        this.logger = logger;
        this.isCancelled = new AtomicBoolean();
        this.builder = builder;

        // the metadata store node does not need session management
        if (builder != null && includeSessionManagement) {
            this.sessionManager = builder.buildSessionManager(
                    Constants.KeepAlives.KeepAliveIntervalMs, this);
        }

        this.nextMsgId = 1;
        this.replyCallbacks = new HashMap<>();
        this.replyDeadlines = new DeadlineCollection<>();
        this.delayedFutures = new DeadlineCollection<>();

        // set up the delay handling to use the scheduled delays of the node
        Futures.Delay = (delayMs) -> delay(delayMs);
    }

    public static NodeType determineType(String nodeId) {
        int nodeOrdinal = Integer.parseInt(nodeId.replace("n", ""));
        if (nodeOrdinal == 1) {
            return NodeType.MetadataStore;
        } else if (nodeOrdinal <= BookieCount + 1) {
            return NodeType.Bookie;
        } else {
            return NodeType.KvStore;
        }
    }

    public static String getKvStoreNode() {
        // 1 metadata store, BookieCount bookies, rest are KV stores
        return "n" + BookieCount + 2;
    }

    public Logger getLogger() {
        return logger;
    }

    public boolean handleTimeout() {
        long now = System.currentTimeMillis();
        while (replyDeadlines.hasNext(now)) {
            JsonNode msg = replyDeadlines.next();

            int msgId = msg.get(Fields.BODY).get(Fields.MSG_ID).asInt();
            if (replyCallbacks.containsKey(msgId)) {
                logger.logDebug("TIMEOUT !" + msg.toString());
                ObjectNode timeOutResponse = mapper.createObjectNode();

                ObjectNode body = mapper.createObjectNode();
                body.put(Fields.MSG_TYPE, msg.get(Fields.BODY).get(Fields.MSG_TYPE).asText());
                body.put(Fields.RC, ReturnCodes.TIME_OUT);
                body.put(Fields.IN_REPLY_TO, msg.get(Fields.BODY).get(Fields.MSG_ID).asText());

                timeOutResponse.set(Fields.BODY, body);
                timeOutResponse.put(Fields.SOURCE, msg.get(Fields.DEST).asText());
                timeOutResponse.put(Fields.DEST, nodeId);

                handleReply(timeOutResponse);
                return true;
            }
        }

        return false;
    }

    public CompletableFuture<Void> delay(int delayMs) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        delayedFutures.add(System.currentTimeMillis() + delayMs, future);
        return future;
    }

    public boolean resumeDelayedTask() {
        long now = System.currentTimeMillis();
        if (delayedFutures.hasNext(now)) {
            CompletableFuture<Void> future = delayedFutures.next();
            future.complete(null);
            return true;
        } else {
            return false;
        }
    }

    public void send(String destId, String command) {
        ObjectNode body = mapper.createObjectNode();
        sendRequest(destId, command, body, null, Constants.Timeouts.TimeoutMs);
    }

    public void send(String destId, String command, ObjectNode body) {
        sendRequest(destId, command, body, null, Constants.Timeouts.TimeoutMs);
    }

    public CompletableFuture<JsonNode> sendRequest(String destId, String command) {
        ObjectNode body = mapper.createObjectNode();
        return sendRequest(destId, command, body);
    }

    public CompletableFuture<JsonNode> sendRequest(String destId, String command, ObjectNode body) {
        CompletableFuture<JsonNode> replyFuture = new CompletableFuture<>();
        sendRequest(destId, command, body, replyFuture, Constants.Timeouts.TimeoutMs);
        return replyFuture;
    }

    public CompletableFuture<JsonNode> sendRequest(String destId, String command, ObjectNode body, int timeoutMs) {
        CompletableFuture<JsonNode> replyFuture = new CompletableFuture<>();
        sendRequest(destId, command, body, replyFuture, timeoutMs);
        return replyFuture;
    }

    private void sendRequest(String destId,
                             String command,
                             ObjectNode sendBody,
                             CompletableFuture<JsonNode> replyFuture,
                             int timeoutMs) {
        nextMsgId++;

        ObjectNode body = sendBody.deepCopy();
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
            replyDeadlines.add(System.currentTimeMillis() + timeoutMs, msg);
        }

        net.write(msg.toString());
    }

    public void reply(JsonNode msg, String returnCode) {
        ObjectNode body = mapper.createObjectNode();
        reply(msg, returnCode, body);
    }

    public void reply(JsonNode msg, String returnCode, JsonNode replyBody) {
        nextMsgId++;
        ObjectNode body = replyBody.deepCopy();

        if (returnCode != null) {
            body.put(Fields.RC, returnCode);
        }
        body.put(Fields.MSG_ID, nextMsgId);
        body.set("in_reply_to", msg.get(Fields.BODY).get(Fields.MSG_ID));
        body.put(Fields.MSG_TYPE, msg.get(Fields.BODY).get(Fields.MSG_TYPE).asText());

        ObjectNode outMsg = mapper.createObjectNode();
        outMsg.put(Fields.SOURCE, nodeId);
        outMsg.put(Fields.DEST, msg.get(Fields.SOURCE).asText());
        outMsg.set(Fields.BODY, body);

        net.write(outMsg.toString());
    }

    protected void replyWithError(JsonNode msg, int errorCode, String errorText) {
        nextMsgId++;
        ObjectNode errorBody = mapper.createObjectNode();
        errorBody.put(Fields.MSG_TYPE, "error");
        errorBody.put("code", errorCode);
        errorBody.put("text", errorText);
        errorBody.put(Fields.MSG_ID, nextMsgId);
        errorBody.set("in_reply_to", msg.get(Fields.BODY).get(Fields.MSG_ID));

        ObjectNode outMsg = mapper.createObjectNode();
        outMsg.put(Fields.SOURCE, nodeId);
        outMsg.put(Fields.DEST, msg.get(Fields.SOURCE).asText());
        outMsg.set(Fields.BODY, errorBody);

        net.write(outMsg.toString());
    }

    protected void proxy(JsonNode msg, String dest) {
        ObjectNode proxiedMsg = (ObjectNode)msg;
        proxiedMsg.put(Fields.DEST, dest);
        net.write(proxiedMsg.toString());
    }

    public void handleReply(JsonNode reply) {
        try {
            int msgId = reply.get(Fields.BODY).get(Fields.IN_REPLY_TO).asInt();
            CompletableFuture<JsonNode> replyCallback = replyCallbacks.get(msgId);

            if (replyCallback != null) {
                replyCallback.complete(reply);
            } else {
                logger.logReplyToTimedOutMsg(reply);
            }

            replyCallbacks.remove(msgId);
        } catch (Throwable t) {
            logger.logError("Failed handling reply.", t);
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
    abstract void printState();
}
