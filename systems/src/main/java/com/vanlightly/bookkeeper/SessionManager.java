package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SessionManager implements RequestHandler {
    private ObjectMapper mapper;
    private MessageSender messageSender;
    private Logger logger;

    private long keepAliveMs;
    private Instant lastSentKeepAlive;
    private AtomicLong sessionId;
    private AtomicBoolean pendingSessionId;
    private AtomicBoolean pendingKeepAlive;
    private List<CompletableFuture<Long>> sessionFutures;

    private static Set<String> requestsHandled = new HashSet<>(
            Arrays.asList(Commands.Metadata.SESSION_EXPIRED)
    );

    public SessionManager(Long keepAliveMs,
                          ObjectMapper mapper,
                          MessageSender messageSender,
                          Logger logger) {
        this.keepAliveMs = keepAliveMs;
        this.mapper = mapper;
        this.messageSender = messageSender;
        this.logger = logger;

        this.pendingSessionId = new AtomicBoolean();
        this.pendingKeepAlive = new AtomicBoolean();
        this.sessionId = new AtomicLong(-1L);
        this.sessionFutures = new ArrayList<>();
        this.lastSentKeepAlive = Instant.now().minus(1, ChronoUnit.DAYS);
    }

    public boolean maintainSession() {
        if (!pendingSessionId.get() && sessionId.get() == -1L) {
            obtainNewSession();
            return true;
        } else if (sessionId.get() > -1L
                && !pendingKeepAlive.get()
                && Duration.between(lastSentKeepAlive, Instant.now()).toMillis() > keepAliveMs) {
            sendKeepAlive();
            return true;
        } else {
            return false;
        }
    }

    public void clearCachedSession() {
        logger.logDebug("Received bad session response, clearing cached session id");
        sessionId.set(-1L);
    }

    public CompletableFuture<Long> getSessionId() {
        if (sessionId.get() > -1L) {
            return CompletableFuture.<Long>completedFuture(sessionId.get());
        } else {
            if (!pendingSessionId.get()) {
                obtainNewSession();
            }

            CompletableFuture<Long> future = new CompletableFuture<>();
            sessionFutures.add(future);

            return future;
        }
    }

    public boolean matchesCurrentSession(JsonNode request) {
        JsonNode body = request.get(Fields.BODY);
        long msgSessionId = body.get(Fields.SESSION_ID).asLong();

        if (sessionId.get() == msgSessionId) {
            return true;
        } else {
            logger.logBadSession(request, msgSessionId, sessionId.get());
            return false;
        }
    }

    private void obtainNewSession() {
        pendingSessionId.set(true);
        lastSentKeepAlive = Instant.now();
        logger.logDebug("Obtaining a new session. Current session is: " + sessionId.get());
        messageSender.sendRequest(Node.MetadataNodeId, Commands.Metadata.SESSION_NEW)
            .thenAccept((JsonNode reply) -> updateSession(reply))
            .whenComplete((Void v, Throwable t) -> {
                if (t != null) {
                    logger.logError("Failed obtaining a new session", t);
                    sessionId.set(-1L);
                    pendingSessionId.set(false);
                }
            });
    }

    private void sendKeepAlive() {
        pendingKeepAlive.set(true);
        ObjectNode ka = mapper.createObjectNode();
        ka.put(Fields.SESSION_ID, sessionId.get());
        messageSender.sendRequest(Node.MetadataNodeId, Commands.Metadata.SESSION_KEEP_ALIVE,
                ka, (int)Constants.KeepAlives.KeepAliveIntervalMs*3)
                .thenAccept((JsonNode msg) -> {
                    String rc = msg.get(Fields.BODY).get(Fields.RC).asText();
                    long session = msg.get(Fields.BODY).path(Fields.SESSION_ID).asLong();
                    if (rc.equals(ReturnCodes.Metadata.BAD_SESSION)
                            && session >= sessionId.get()
                            && !pendingSessionId.get()) {
                        logger.logDebug("Keep Alive response of bad session " + session);
                        obtainNewSession();
                    }
                    // timeouts and ok return codes can be ignored
                })
                .whenComplete((Void v, Throwable t) -> {
                    if (t != null) {
                        logger.logError("Keep alive failed", t);
                    }
                    pendingKeepAlive.set(false);
                });
        lastSentKeepAlive = Instant.now();
    }

    private void updateSession(JsonNode msg) {
        JsonNode body = msg.get(Fields.BODY);
        String rc = body.get(Fields.RC).asText();

        if (rc.equals(ReturnCodes.OK)) {
            long newSessionId = body.get(Fields.SESSION_ID).asLong();
            if (newSessionId > sessionId.get()) {
                logger.logDebug("Obtained new session: " + newSessionId);
                sessionId.set(newSessionId);

                for (CompletableFuture<Long> future : sessionFutures) {
                    future.complete(sessionId.get());
                }

                sessionFutures.clear();
            } else {
                logger.logBadSession(msg, newSessionId, sessionId.get());
            }
        } else {
            logger.logDebug("Could not establish new session. Code: " + rc);
            sessionId.set(-1L);
        }

        pendingSessionId.set(false);
    }

    @Override
    public boolean handlesRequest(String requestType) {
        return requestsHandled.contains(requestType);
    }

    @Override
    public void handleRequest(JsonNode request) {
        String type = request.get(Fields.BODY).get(Fields.MSG_TYPE).asText();

        switch (type) {
            case Commands.Metadata.SESSION_EXPIRED:
                handleExpiredSession(request);
                break;
            default:
                logger.logError("Unsupported command type" + request);
        }
    }

    private void handleExpiredSession(JsonNode request) {
        if (matchesCurrentSession(request)) {
            logger.logDebug("Received session expired notification");
            sessionId.set(-1L);
        }
    }
}
