package com.vanlightly.bookkeeper.kv.bkclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.Commands;
import com.vanlightly.bookkeeper.Fields;
import com.vanlightly.bookkeeper.MessageSender;
import com.vanlightly.bookkeeper.ReturnCodes;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class PendingAddOp {
    ObjectMapper mapper;
    Entry entry;
    int pendingAdds;
    Set<Integer> successAdds;
    List<String> ensemble;
    int writeQuorum;
    int ackQuorum;

    MessageSender messageSender;
    LedgerHandle lh;
    CompletableFuture<Entry> callerFuture;
    boolean aborted;

    public PendingAddOp(ObjectMapper mapper,
                        MessageSender messageSender,
                        Entry entry,
                        List<String> ensemble,
                        int writeQuorum,
                        int ackQuorum,
                        LedgerHandle lh,
                        CompletableFuture<Entry> callerFuture) {
        this.mapper = mapper;
        this.messageSender = messageSender;
        this.entry = entry;
        this.ensemble = ensemble;
        this.writeQuorum = writeQuorum;
        this.ackQuorum = ackQuorum;
        this.pendingAdds = ensemble.size();
        this.successAdds = new HashSet<>();
        this.lh = lh;
        this.callerFuture = callerFuture;
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

        String bookieId = ensemble.get(bookieIndex);
        messageSender.sendRequest(bookieId, Commands.Bookie.ADD_ENTRY, body)
                .thenAccept((JsonNode reply) -> handleReply(reply));
        pendingAdds++;
    }

    public void handleReply(JsonNode reply) {
        if (aborted) {
            // ignores any pending responses
            return;
        }

        JsonNode body = reply.get(Fields.BODY);
        String bookieId = reply.get(Fields.SOURCE).asText();
        String rc = body.get(Fields.RC).asText();
        int bookieIndex = ensemble.indexOf(bookieId);
        pendingAdds--;

        if (rc.equals(ReturnCodes.OK)) {
            successAdds.add(bookieIndex);
            lh.sendAddSuccessCallbacks();
        } else if (rc.equals(ReturnCodes.Bookie.FENCED)) {
            lh.handleUnrecoverableErrorDuringAdd(rc);
        } else {
            Map<Integer,String> failedBookies = new HashMap<>();
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

    public void abort() {
        aborted = true;
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
