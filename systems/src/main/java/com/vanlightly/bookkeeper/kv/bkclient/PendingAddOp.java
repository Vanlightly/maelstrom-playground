package com.vanlightly.bookkeeper.kv.bkclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.util.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.vanlightly.bookkeeper.util.Futures.Delay;

public class PendingAddOp {
    private Logger logger = LogManager.getLogger(this.getClass().getSimpleName());
    private ObjectMapper mapper = MsgMapping.getMapper();
    private Entry entry;
    private Set<Integer> successAdds;
    private List<String> ensemble;
    private int writeQuorum;
    private int ackQuorum;

    private MessageSender messageSender;
    private LedgerWriteHandle lh;
    private boolean isRecoveryAdd;
    private CompletableFuture<Entry> callerFuture;
    private AtomicBoolean isCancelled;
    private boolean aborted;

    public PendingAddOp(MessageSender messageSender,
                        Entry entry,
                        List<String> ensemble,
                        int writeQuorum,
                        int ackQuorum,
                        LedgerWriteHandle lh,
                        boolean isRecoveryAdd,
                        CompletableFuture<Entry> callerFuture,
                        AtomicBoolean isCancelled) {
        this.messageSender = messageSender;
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
        // TODO: REMOVE => use delays to increase probability of read/write overlap
        for (int i=0; i<writeQuorum; i++) {
            int finalI = i;
            Delay.apply(Config.AddReadSpreadMs*i).thenRun(() -> {
                sendAddEntryRequest(finalI);
            });
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
        logger.logDebug("Send ADD"
                + " ledgerId: " + entry.getLedgerId() + " entryId: " + entry.getEntryId()
                + " lac: " + lh.getLastAddConfirmed()
                + " to bookie: " + bookieId + " index: " + bookieIndex
                + " of ensemble: " + ensemble);
        messageSender.sendRequest(bookieId, Commands.Bookie.ADD_ENTRY, body)
                .thenApply(this::checkForCancellation)
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
            logger.logDebug("PendingAddOp received a fenced response.");
            lh.handleUnrecoverableErrorDuringAdd(rc, isRecoveryAdd);
        } else {
            logger.logDebug("PendingAddOp received failure response code: " + rc);
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

    private <T> T checkForCancellation(T t) {
        if (isCancelled.get()) {
            throw new OperationCancelledException();
        }

        return t;
    }
}
