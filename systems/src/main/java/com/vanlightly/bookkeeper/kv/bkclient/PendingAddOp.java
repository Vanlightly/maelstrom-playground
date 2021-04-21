package com.vanlightly.bookkeeper.kv.bkclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class PendingAddOp {
    ObjectMapper mapper;
    Logger logger;
    Entry entry;
    Set<Integer> successAdds;
    List<String> ensemble;
    int writeQuorum;
    int ackQuorum;

    MessageSender messageSender;
    LedgerHandle lh;
    boolean isRecoveryAdd;
    CompletableFuture<Entry> callerFuture;
    AtomicBoolean isCancelled;
    boolean aborted;

    public PendingAddOp(ObjectMapper mapper,
                        MessageSender messageSender,
                        Logger logger,
                        Entry entry,
                        List<String> ensemble,
                        int writeQuorum,
                        int ackQuorum,
                        LedgerHandle lh,
                        boolean isRecoveryAdd,
                        CompletableFuture<Entry> callerFuture,
                        AtomicBoolean isCancelled) {
        this.mapper = mapper;
        this.messageSender = messageSender;
        this.logger = logger;
        this.entry = entry;
        this.ensemble = ensemble;
        this.writeQuorum = writeQuorum;
        this.ackQuorum = ackQuorum;
        this.successAdds = new HashSet<>();
        this.lh = lh;
        this.isRecoveryAdd = isRecoveryAdd;
        this.callerFuture = callerFuture;
        this.isCancelled = isCancelled;
    }

    public void begin() {
        for (int i=0; i<writeQuorum; i++) {
            sendAddEntryRequest(i);
        }
    }

    public void sendAddEntryRequest(int bookieIndex) {
        ObjectNode body = mapper.createObjectNode();
        body.put(Fields.L.LEDGER_ID, entry.getLedgerId());
        body.put(Fields.L.ENTRY_ID, entry.getEntryId());
        body.put(Fields.L.VALUE, entry.getValue());
        body.put(Fields.L.LAC, lh.getLastAddConfirmed());
        body.put(Fields.L.RECOVERY, isRecoveryAdd);

        String bookieId = ensemble.get(bookieIndex);
        messageSender.sendRequest(bookieId, Commands.Bookie.ADD_ENTRY, body)
                .thenAccept((JsonNode reply) -> handleReply(reply))
                .whenComplete((Void v, Throwable t) -> {
                    if (t != null) {
                        logger.logError("Add operation failed", t);
                        completeCallerFuture(ReturnCodes.UNEXPECTED_ERROR);
                        aborted = true;
                    }
                });
    }

    public void handleReply(JsonNode reply) {
        if (isCancelled.get() || aborted) {
            callerFuture.completeExceptionally(new OperationCancelledException());
            return;
        }

        JsonNode body = reply.get(Fields.BODY);
        String bookieId = reply.get(Fields.SOURCE).asText();
        String rc = body.get(Fields.RC).asText();
        int bookieIndex = ensemble.indexOf(bookieId);

        if (rc.equals(ReturnCodes.OK)) {
            successAdds.add(bookieIndex);
            lh.sendAddSuccessCallbacks();
        } else if (rc.equals(ReturnCodes.Bookie.FENCED)) {
            lh.handleUnrecoverableErrorDuringAdd(rc);
        } else {
            Map<Integer, String> failedBookies = new HashMap<>();
            failedBookies.put(bookieIndex, bookieId);
            lh.handleBookieFailure(failedBookies);
        }
    }

    public boolean isCommitted() {
        return successAdds.size() >= ackQuorum;
    }

    public Entry getEntry() {
        return entry;
    }

    public void completeCallerFuture(String rc) {
        switch (rc) {
            case ReturnCodes.OK:
                callerFuture.complete(entry);
                break;
            default:
                callerFuture.completeExceptionally(new BkException("Add failed", rc));
        }
    }

    void unsetSuccessAndSendWriteRequest(List<String> ensemble, int bookieIndex) {
        this.ensemble = ensemble;
        sendAddEntryRequest(bookieIndex);

        // may have previously received a response from this bookie but due to
        // a failure of a different write we are changing the ensemble and must
        // remove this bookie from the successful bookies
        if (successAdds.contains(bookieIndex)) {
            successAdds.remove(bookieIndex);
        }
    }
}
