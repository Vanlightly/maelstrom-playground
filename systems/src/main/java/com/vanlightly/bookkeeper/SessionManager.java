package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SessionManager implements RequestHandler {
    private ObjectMapper mapper;
    private MessageSender messageSender;
    private Logger logger;
    private AtomicBoolean isCancelled;

    private boolean sendExplicitKeepAlives;
    private long keepAliveMs;
    private Instant lastSentKeepAlive;
    private AtomicLong sessionId;
    private AtomicBoolean pendingSessionId;
//    private ScheduledExecutorService keepAliveExecutor;
    private List<CompletableFuture<Long>> sessionFutures;

    private static Set<String> requestsHandled = new HashSet<>(
            Arrays.asList(Commands.Metadata.SESSION_EXPIRED)
    );

    public SessionManager(Long keepAliveMs,
                          boolean sendExplicitKeepAlives,
                          ObjectMapper mapper,
                          MessageSender messageSender,
                          Logger logger,
                          AtomicBoolean isCancelled) {
        this.keepAliveMs = keepAliveMs;
        this.sendExplicitKeepAlives = sendExplicitKeepAlives;
        this.mapper = mapper;
        this.messageSender = messageSender;
        this.logger = logger;
        this.isCancelled = isCancelled;

        this.pendingSessionId = new AtomicBoolean();
        this.sessionId = new AtomicLong(-1L);
        this.sessionFutures = new ArrayList<>();
        this.lastSentKeepAlive = Instant.now().minus(1, ChronoUnit.DAYS);
    }

    public boolean shouldMaintainSession() {
        return (sessionId.get() == -1L && !pendingSessionId.get())
                || (sendExplicitKeepAlives && Duration.between(lastSentKeepAlive, Instant.now()).toMillis() > keepAliveMs);
    }

    public boolean maintainSession() {
        if (shouldMaintainSession()) {
            if (sessionId.get() > -1) {
                if (sendExplicitKeepAlives) {
                    ObjectNode ka = mapper.createObjectNode();
                    ka.put(Fields.SESSION_ID, sessionId.get());
                    messageSender.send(Node.MetadataNodeId, Commands.Metadata.SESSION_KEEP_ALIVE, ka);
                    lastSentKeepAlive = Instant.now();
                }
            } else if (!pendingSessionId.get()) {
                obtainNewSession();
            }
            return true;
        } else {
            return false;
        }
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
        lastSentKeepAlive = Instant.now();
        pendingSessionId.set(true);
        messageSender.sendRequest(Node.MetadataNodeId, Commands.Metadata.SESSION_NEW)
            .thenAccept((JsonNode reply) -> updateSession(reply));
    }

    private void updateSession(JsonNode msg) {
        JsonNode body = msg.get(Fields.BODY);
        String rc = body.get(Fields.RC).asText();

        if (rc.equals(ReturnCodes.OK)) {
            long newSessionId = body.get(Fields.SESSION_ID).asLong();
            if (newSessionId > sessionId.get()) {
                sessionId.set(newSessionId);

                for (CompletableFuture<Long> future : sessionFutures) {
                    future.complete(sessionId.get());
                }

                sessionFutures.clear();
            } else {
                logger.logBadSession(msg, newSessionId, sessionId.get());
            }
        } else {
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
            sessionId.set(-1L);
        }
    }
}
