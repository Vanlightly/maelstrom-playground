package com.vanlightly.bookkeeper.kv.bkclient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.Futures;
import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class LedgerManager {
    private Logger logger = LogManager.getLogger(this.getClass().getName());
    private ObjectMapper mapper;
    private SessionManager sessionManager;
    private MessageSender messageSender;
    private AtomicBoolean isCancelled;

    public LedgerManager(ObjectMapper mapper,
                         SessionManager sessionManager,
                         MessageSender messageSender,
                         AtomicBoolean isCancelled) {
        this.mapper = mapper;
        this.sessionManager = sessionManager;
        this.messageSender = messageSender;
        this.isCancelled = isCancelled;
    }

    public CompletableFuture<List<String>> getAvailableBookies() {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        Futures.retryTransient(future, isCancelled, () -> doGetAvailableBookies());
        return future;
    }

    private CompletableFuture<List<String>> doGetAvailableBookies() {
        CompletableFuture<List<String>> future = new CompletableFuture<>();

        sessionManager.getSessionId()
                .thenCompose((sessionId) -> {
                    ObjectNode body = mapper.createObjectNode();
                    body.put(Fields.SESSION_ID, sessionId);
                    return messageSender.sendRequest(Node.MetadataNodeId, Commands.Metadata.BK_METADATA_READ, body);
                })
                .thenAccept((JsonNode msg) -> {
                    JsonNode body = msg.get(Fields.BODY);
                    String rc = body.get(Fields.RC).asText();

                    switch (rc) {
                        case ReturnCodes.OK:
                            List<String> bookies = new ArrayList<>();
                            for (JsonNode b : (ArrayNode)body.get("available_bookies")) {
                                bookies.add(b.asText());
                            }
                            future.complete(bookies);
                            break;
                        case ReturnCodes.TIME_OUT:
                            future.completeExceptionally(new TransientException("Operation timed out"));
                            break;
                        case ReturnCodes.Metadata.BAD_SESSION:
                            sessionManager.clearCachedSession();
                            future.completeExceptionally(new TransientException("Session expired"));
                            break;
                        default:
                            future.completeExceptionally(new MetadataException("Failed to read available bookies", rc));
                    }
                });

        return future;
    }

    public CompletableFuture<Versioned<LedgerMetadata>> getLedgerMetadata(long ledgerId) {
        CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();
        Futures.retryTransient(future, isCancelled, () -> doGetLedgerMetadata(ledgerId));
        return future;
    }

    private CompletableFuture<Versioned<LedgerMetadata>> doGetLedgerMetadata(long ledgerId) {
        CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();

        sessionManager.getSessionId()
                .thenCompose((sessionId) -> {
                    ObjectNode body = mapper.createObjectNode();
                    body.put(Fields.SESSION_ID, sessionId);
                    body.put(Fields.L.LEDGER_ID, ledgerId);
                    return messageSender.sendRequest(Node.MetadataNodeId, Commands.Metadata.LEDGER_READ, body);
                })
                .thenAccept((JsonNode reply) -> {
                    JsonNode body = reply.get(Fields.BODY);
                    String rc = body.get(Fields.RC).asText();

                    switch (rc) {
                        case ReturnCodes.OK:
                            try {
                                LedgerMetadata md = mapper.treeToValue(body.get(Fields.M.LEDGER_METADATA), LedgerMetadata.class);
                                long version = body.get(Fields.VERSION).asLong();
                                future.complete(new Versioned<>(md, version));
                            } catch (JsonProcessingException e) {
                                future.completeExceptionally(e);
                            }
                            break;
                        case ReturnCodes.TIME_OUT:
                            future.completeExceptionally(new TransientException("Operation timed out"));
                            break;
                        case ReturnCodes.Metadata.BAD_SESSION:
                            sessionManager.clearCachedSession();
                            future.completeExceptionally(new TransientException("Session expired"));
                            break;
                        default:
                            future.completeExceptionally(new MetadataException("Failed to read ledger metadata", rc));
                    }
                })
            .whenComplete((Void v, Throwable t) -> {
                if (t != null) {
                    logger.logError("Failed reading ledger metadata", t);
                }
            });

        return future;
    }

    public CompletableFuture<Long> getLedgerId() {
        CompletableFuture<Long> future = new CompletableFuture<>();
        Futures.retryTransient(future, isCancelled, () -> doGetLedgerId());
        return future;
    }

    public CompletableFuture<Long> doGetLedgerId() {
        CompletableFuture<Long> future = new CompletableFuture<>();

        sessionManager.getSessionId()
                .thenCompose((sessionId) -> {
                    ObjectNode body = mapper.createObjectNode();
                    body.put(Fields.SESSION_ID, sessionId);
                    return messageSender.sendRequest(Node.MetadataNodeId, Commands.Metadata.GET_LEDGER_ID, body);
                })
                .thenAccept((JsonNode reply) -> {
                    JsonNode body = reply.get(Fields.BODY);
                    String rc = body.get(Fields.RC).asText();

                    switch (rc) {
                        case ReturnCodes.OK:
                            long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
                            future.complete(ledgerId);
                            break;
                        case ReturnCodes.TIME_OUT:
                            future.completeExceptionally(new TransientException("Operation timed out"));
                            break;
                        case ReturnCodes.Metadata.BAD_SESSION:
                            sessionManager.clearCachedSession();
                            future.completeExceptionally(new TransientException("Session expired"));
                            break;
                        default:
                            future.completeExceptionally(new MetadataException("Failed to get ledger id", rc));
                    }
                });

        return future;
    }

    public CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(LedgerMetadata ledgerMetadata) {
        CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();

        Futures.retryTransient(future, isCancelled,
                () -> doWriteLedgerMetadata(ledgerMetadata, -1L, Commands.Metadata.LEDGER_CREATE));

        return future;
    }

    public CompletableFuture<Versioned<LedgerMetadata>> updateLedgerMetadata(Versioned<LedgerMetadata> ledgerMetadata) {
        CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();

        Futures.retryTransient(future, isCancelled,
                () -> doWriteLedgerMetadata(ledgerMetadata.getValue(), ledgerMetadata.getVersion(), Commands.Metadata.LEDGER_UPDATE));

        return future;
    }

    private CompletableFuture<Versioned<LedgerMetadata>> doWriteLedgerMetadata(LedgerMetadata ledgerMetadata,
                                                                             long version,
                                                                             String command) {
        CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();

        sessionManager.getSessionId()
                .thenCompose((sessionId) -> {
                    ObjectNode body = mapper.createObjectNode();
                    body.put(Fields.SESSION_ID, sessionId);
                    if (version > -1L) {
                        body.put(Fields.VERSION, version);
                    }
                    body.set(Fields.M.LEDGER_METADATA, mapper.valueToTree(ledgerMetadata));
                    return messageSender.sendRequest(Node.MetadataNodeId, command, body);
                })
                .thenAccept((JsonNode reply) -> {
                    JsonNode body = reply.get(Fields.BODY);
                    String rc = body.get(Fields.RC).asText();

                    switch (rc) {
                        case ReturnCodes.OK:
                            try {
                                LedgerMetadata md = mapper.treeToValue(body.get(Fields.M.LEDGER_METADATA), LedgerMetadata.class);
                                long v = body.get(Fields.VERSION).asLong();
                                future.complete(new Versioned<>(md, v));
                            } catch (JsonProcessingException e) {
                                future.completeExceptionally(e);
                            }
                            break;
                        case ReturnCodes.TIME_OUT:
                            future.completeExceptionally(new TransientException("Operation timed out"));
                            break;
                        case ReturnCodes.Metadata.BAD_SESSION:
                            sessionManager.clearCachedSession();
                            future.completeExceptionally(new TransientException("Session expired"));
                            break;
                        case ReturnCodes.Metadata.BAD_VERSION:
                            future.completeExceptionally(new MetadataException("Ledger netadata write operation failed due to a bad version. Command: " + command, rc));
                            break;
                        default:
                            future.completeExceptionally(new MetadataException("Failed to write ledger metadata with command: " + command, rc));
                    }
                })
                .whenComplete((Void v, Throwable t) -> {
                    if (t != null) {
                        logger.logError("Failed writing ledger metadata", t);
                    }
                });

        return future;
    }
}
