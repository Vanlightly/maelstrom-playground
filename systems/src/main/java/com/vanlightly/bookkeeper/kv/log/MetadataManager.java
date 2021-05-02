package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.Futures;
import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class MetadataManager {
    private Logger logger = LogManager.getLogger(this.getClass().getName());
    private ObjectMapper mapper;
    private SessionManager sessionManager;
    private MessageSender messageSender;
    private AtomicBoolean isCancelled;

    public MetadataManager(ObjectMapper mapper,
                           SessionManager sessionManager,
                           MessageSender messageSender,
                           AtomicBoolean isCancelled) {
        this.mapper = mapper;
        this.sessionManager = sessionManager;
        this.messageSender = messageSender;
        this.isCancelled = isCancelled;
    }

    public CompletableFuture<Versioned<String>> getLeader() {
        CompletableFuture<Versioned<String>> future = new CompletableFuture<>();
        Futures.retryTransient(future, isCancelled, () -> doGetLeader());
        return future;
    }

    private CompletableFuture<Versioned<String>> doGetLeader() {
        CompletableFuture<Versioned<String>> future = new CompletableFuture<>();

        sessionManager.getSessionId()
                .thenCompose((sessionId) -> {
                    ObjectNode body = mapper.createObjectNode();
                    body.put(Fields.SESSION_ID, sessionId);
                    return messageSender.sendRequest(Node.MetadataNodeId, Commands.Metadata.GET_LEADER_ID, body);
                })
                .thenAccept((JsonNode msg) -> {
                    JsonNode body = msg.get(Fields.BODY);
                    String rc = body.get(Fields.RC).asText();

                    switch (rc) {
                        case ReturnCodes.OK:
                            String leader = body.get(Fields.KV.LEADER).asText();
                            int leaderVersion = body.get(Fields.KV.LEADER_VERSION).asInt();
                            future.complete(new Versioned<>(leader, leaderVersion));
                            break;
                        case ReturnCodes.TIME_OUT:
                            future.completeExceptionally(new TransientException("Operation timed out"));
                            break;
                        case ReturnCodes.Metadata.BAD_SESSION:
                            sessionManager.clearCachedSession();
                            future.completeExceptionally(new TransientException("Session expired"));
                            break;
                        default:
                            future.completeExceptionally(new MetadataException("Failed to read leader id", rc));
                    }
                });

        return future;
    }

    public CompletableFuture<Versioned<List<Long>>> getLedgerList() {
        CompletableFuture<Versioned<List<Long>>> future = new CompletableFuture<>();
        Futures.retryTransient(future, isCancelled, () -> doGetLedgerList());
        return future;
    }

    private CompletableFuture<Versioned<List<Long>>> doGetLedgerList() {
        CompletableFuture<Versioned<List<Long>>> future = new CompletableFuture<>();

        logger.logDebug("Get ledger list");

        sessionManager.getSessionId()
                .thenCompose((sessionId) -> {
                    ObjectNode body = mapper.createObjectNode();
                    body.put(Fields.SESSION_ID, sessionId);
                    return messageSender.sendRequest(Node.MetadataNodeId, Commands.Metadata.GET_LEDGER_LIST, body);
                })
                .thenAccept((JsonNode msg) -> {
                    JsonNode body = msg.get(Fields.BODY);
                    String rc = body.get(Fields.RC).asText();

                    logger.logDebug("Get ledger list response: " + rc);

                    switch (rc) {
                        case ReturnCodes.OK:
                            List<Long> ledgerList = new ArrayList<>();
                            for (JsonNode l : (ArrayNode)body.get(Fields.KV.LEDGER_LIST)) {
                                ledgerList.add(l.asLong());
                            }
                            long llVersion = body.get(Fields.KV.LEDGER_LIST_VERSION).asLong();
                            Versioned<List<Long>> versionedLedgerList = new Versioned<List<Long>>(ledgerList, llVersion);
                            future.complete(versionedLedgerList);
                            break;
                        case ReturnCodes.TIME_OUT:
                            future.completeExceptionally(new TransientException("Operation timed out"));
                            break;
                        case ReturnCodes.Metadata.BAD_SESSION:
                            sessionManager.clearCachedSession();
                            future.completeExceptionally(new TransientException("Session expired"));
                            break;
                        default:
                            future.completeExceptionally(new MetadataException("Failed to read ledger list", rc));
                    }
                });

        return future;
    }

    public CompletableFuture<Versioned<List<Long>>> updateLedgerList(Versioned<List<Long>> ledgerList) {
        CompletableFuture<Versioned<List<Long>>> future = new CompletableFuture<>();
        Futures.retryTransient(future, isCancelled, () -> doUpdateLedgerList(ledgerList));
        return future;
    }

    private CompletableFuture<Versioned<List<Long>>> doUpdateLedgerList(Versioned<List<Long>> ledgerList) {
        CompletableFuture<Versioned<List<Long>>> future = new CompletableFuture<>();

        sessionManager.getSessionId()
                .thenCompose((sessionId) -> {
                    ObjectNode body = mapper.createObjectNode();
                    body.put(Fields.SESSION_ID, sessionId);
                    body.put(Fields.KV.LEDGER_LIST_VERSION, ledgerList.getVersion());

                    ArrayNode jsonList = mapper.createArrayNode();
                    for (long ledgerId : ledgerList.getValue()) {
                        jsonList.add(ledgerId);
                    }
                    body.set(Fields.KV.LEDGER_LIST, jsonList);

                    return messageSender.sendRequest(Node.MetadataNodeId, Commands.Metadata.LEDGER_LIST_UPDATE, body);
                })
                .thenAccept((JsonNode msg) -> {
                    JsonNode body = msg.get(Fields.BODY);
                    String rc = body.get(Fields.RC).asText();

                    switch (rc) {
                        case ReturnCodes.OK:
                            List<Long> ll = new ArrayList<>();
                            for (JsonNode l : (ArrayNode)body.get(Fields.KV.LEDGER_LIST)) {
                                ll.add(l.asLong());
                            }
                            long version = body.get(Fields.KV.LEDGER_LIST_VERSION).asLong();
                            future.complete(new Versioned<List<Long>>(ll, version));
                            break;
                        case ReturnCodes.TIME_OUT:
                            future.completeExceptionally(new TransientException("Operation timed out"));
                            break;
                        case ReturnCodes.Metadata.BAD_SESSION:
                            sessionManager.clearCachedSession();
                            future.completeExceptionally(new TransientException("Session expired"));
                            break;
                        case ReturnCodes.Metadata.BAD_VERSION:
                            future.completeExceptionally(new MetadataException("Ledger list bad version", ReturnCodes.Metadata.BAD_VERSION));
                            break;
                        default:
                            future.completeExceptionally(new MetadataException("Failed to update the ledger list", rc));
                    }
                });

        return future;
    }
}
