package com.vanlightly.bookkeeper.kv.bkclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.LedgerStatus;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.Futures;
import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;
import com.vanlightly.bookkeeper.util.MsgMapping;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class LedgerReadHandle {
    private Logger logger = LogManager.getLogger(this.getClass().getName());
    private ObjectMapper mapper = MsgMapping.getMapper();
    private MessageSender messageSender;
    private AtomicBoolean isCancelled;

    Versioned<LedgerMetadata> versionedMetadata;
    long lastAddConfirmed;
    LedgerManager ledgerManager;

    public LedgerReadHandle(LedgerManager ledgerManager,
                            MessageSender messageSender,
                            Versioned<LedgerMetadata> versionedMetadata) {
        this.ledgerManager = ledgerManager;
        this.messageSender = messageSender;
        // must be a copy as sharing between handles during recovery can cause inconsistency
        this.versionedMetadata = new Versioned<>(versionedMetadata.getValue(),
                versionedMetadata.getVersion());
        this.isCancelled = new AtomicBoolean();

        if (versionedMetadata.getValue().getStatus() == LedgerStatus.CLOSED) {
            this.lastAddConfirmed = versionedMetadata.getValue().getLastEntryId();
        } else {
            this.lastAddConfirmed = -1L;
        }
    }

    public void cancel() {
        isCancelled.set(true);
    }

    public void printState() {
        logger.logInfo("------------ Ledger Read Handle State -------------" + System.lineSeparator()
            + "Ledger metadata version: " + versionedMetadata.getVersion() + System.lineSeparator()
            + "Ledger metadata: " + versionedMetadata.getValue() + System.lineSeparator()
            + "lastAddConfirmed: "+ lastAddConfirmed + System.lineSeparator()
            + "----------------------------------------------");
    }

    public Versioned<LedgerMetadata> getCachedLedgerMetadata() {
        return versionedMetadata;
    }

    public void setCachedLedgerMetadata(Versioned<LedgerMetadata> updatedMetadata) {
        versionedMetadata = updatedMetadata;
    }

    public long getLedgerId() {
        return versionedMetadata.getValue().getLedgerId();
    }

    public long getLastAddConfirmed() {
        return lastAddConfirmed;
    }

    public void setLastAddConfirmed(long lastAddConfirmed) {
        this.lastAddConfirmed = lastAddConfirmed;
    }

    public CompletableFuture<Result<Entry>> read(long entryId) {
        Decider readDecider = new Decider(this,
                lm().getWriteQuorum(),
                1,
                lm().getWriteQuorum(),
                lm().getWriteQuorum());
        // TODO: implement sticky bookie reads optimization
        return sequentialRead(entryId, 0, readDecider);
    }

    /*
        Performs a read against a single bookie. If a non-success response is received, it
        sequentially tries the next bookie until either a success response or there are no
        more bookies left to try
     */
    private CompletableFuture<Result<Entry>> sequentialRead(long entryId,
                                                            int bookieIndex,
                                                            Decider decider) {
        ObjectNode readReq = mapper.createObjectNode();
        readReq.put(Fields.L.LEDGER_ID, lm().getLedgerId());
        readReq.put(Fields.L.ENTRY_ID, entryId);
        String bookieId = lm().getEnsembleFor(entryId).get(bookieIndex);

        return messageSender.sendRequest(bookieId, Commands.Bookie.READ_ENTRY, readReq)
                .thenApply(this::checkForCancellation)
                .thenCompose((JsonNode reply) -> {
                    JsonNode body = reply.get(Fields.BODY);
                    String rc = body.get(Fields.RC).asText();
                    decider.register(rc, body);

                    if (decider.isPositive()) {
                        return CompletableFuture.completedFuture(new Result<>(ReturnCodes.OK, decider.getEntry()));
                    } else {
                        int nextBookieIndex = bookieIndex + 1;
                        if (nextBookieIndex >= lm().getWriteQuorum()) {
                            if (decider.isNegative()) {
                                return CompletableFuture.completedFuture(new Result<>(ReturnCodes.Bookie.NO_SUCH_ENTRY, null));
                            } else {
                                return CompletableFuture.completedFuture(new Result<>(ReturnCodes.Ledger.UNKNOWN, null));
                            }
                        } else {
                            return sequentialRead(entryId, nextBookieIndex, decider);
                        }
                    }
                });
    }

    public CompletableFuture<Result<Entry>> recoveryRead(long entryId) {
        // a recovery read only needs an AQ to succeed, but a negative
        // requires QC as this then precludes the possibility of a success
        Decider recoveryReadDecider = new Decider(this,
                lm().getWriteQuorum(),
                lm().getAckQuorum(),   // positive threshold
                quorumCoverage(lm()),  // negative threshold
                quorumCoverage(lm())); // unknown threshold
        return parallelRead(entryId, Commands.Bookie.READ_ENTRY, true, recoveryReadDecider);
    }

    public CompletableFuture<Result<Entry>> readLacWithFencing() {
        // for fencing LAC to be complete we must ensure that no AQ of bookies
        // remains unfenced, hence the QC threshold for positive and AQ for unknown
        Decider fencingDecider = new Decider(this,
                lm().getWriteQuorum(),
                quorumCoverage(lm()),  // positive threshold
                lm().getWriteQuorum(), // negative threshold (not possible with fencing LAC read)
                lm().getAckQuorum());  // unknown threshold
        return parallelRead(-1L, Commands.Bookie.READ_LAC, true, fencingDecider);
    }

    public CompletableFuture<Result<Entry>> lacLongPollRead() {
        Decider lpDecider = new LongPollDecider(this,
                lm().getWriteQuorum(),
                quorumCoverage(lm()),
                quorumCoverage(lm()),
                lastAddConfirmed);
        return parallelRead(lastAddConfirmed, Commands.Bookie.READ_LAC_LONG_POLL,
                false, lpDecider);
    }

    /*
        Sends a read all the whole ensemble in parallel and returns the entry with the highest LAC
        as long as enough bookies respond positively.
     */
    public CompletableFuture<Result<Entry>> parallelRead(long entryId,
                                                         String readCommand,
                                                         boolean fence,
                                                         Decider decider) {
        CompletableFuture<Result<Entry>> readFuture = new CompletableFuture<>();

        ObjectNode readReq = mapper.createObjectNode();
        readReq.put(Fields.L.LEDGER_ID, versionedMetadata.getValue().getLedgerId());

        int msgTimeout = Constants.Timeouts.TimeoutMs;
        if (readCommand.equals(Commands.Bookie.READ_LAC_LONG_POLL)) {
            readReq.put(Fields.L.PREVIOUS_LAC, entryId);
            readReq.put(Fields.L.LONG_POLL_TIMEOUT_MS, Constants.KvStore.LongPollTimeoutMs);
            msgTimeout = Constants.KvStore.LongPollResponseTimeoutMs;
        }

        readReq.put(Fields.L.ENTRY_ID, entryId);

        if (fence) {
            readReq.put(Fields.L.FENCE, true);
        }

        List<String> bookies = lm().getEnsembleFor(entryId);
        int writeQuorum = lm().getWriteQuorum();

        for (int b=0; b<writeQuorum; b++) {
            String bookieId = bookies.get(b);
            messageSender.sendRequest(bookieId, readCommand, readReq, msgTimeout)
                    .thenApply(this::checkForCancellation)
                    .thenAccept((JsonNode reply) -> {
                        if (readFuture.isDone()) {
                            // seen enough responses already
                            return;
                        }

                        JsonNode body = reply.get(Fields.BODY);
                        String rc = body.get(Fields.RC).asText();
                        decider.register(rc, body);

                        if (decider.isPositive()) {
                            readFuture.complete(new Result<>(ReturnCodes.OK, decider.getEntry()));
                        } else if (decider.isNegative()) {
                            logger.logDebug("LedgerHandle: Parallel read is negative for entry: " + entryId + " with negatives=" + decider.negatives);
                            readFuture.complete(new Result<>(ReturnCodes.Ledger.NO_QUORUM, null));
                        } else if (decider.isUnknown()) {
                            readFuture.complete(new Result<>(ReturnCodes.Ledger.UNKNOWN, null));
                        }
                    })
                    .whenComplete((Void v, Throwable t) -> {
                        if (t != null) {
                            logger.logError("LedgerHandle: Failed performing parallel read", t);
                            if (!readFuture.isDone()) {
                                readFuture.completeExceptionally(Futures.unwrap(t));
                            }
                        }
                    });

        }

        return readFuture;
    }

    private int quorumCoverage(LedgerMetadata lm) {
        return (lm.getWriteQuorum() - lm.getAckQuorum()) + 1;
    }

    void updateLac(long entryId) {
        if (entryId > lastAddConfirmed) {
            lastAddConfirmed = entryId;
        }
    }

    private LedgerMetadata lm() {
        return versionedMetadata.getValue();
    }

    private <T> T checkForCancellation(T t) {
        if (isCancelled.get()) {
            throw new OperationCancelledException();
        }

        return t;
    }

    private static class Decider {
        int unknowns;
        int positives;
        int negatives;
        Entry intermediateEntry;

        LedgerReadHandle lh;
        int writeQuorum;
        int positiveThreshold;
        int negativeThreshold;
        int unknownThreshold;

        public Decider(LedgerReadHandle lh,
                       int writeQuorum,
                       int positiveThreshold,
                       int negativeThreshold,
                       int unknownThreshold) {
            this.lh = lh;
            this.writeQuorum = writeQuorum;
            this.positiveThreshold = positiveThreshold;
            this.negativeThreshold = negativeThreshold;
            this.unknownThreshold = unknownThreshold;
        }

        public void register(String rc, JsonNode body) {
            if (rc.equals(ReturnCodes.OK)) {
                long lac = body.get(Fields.L.LAC).asLong();
                lh.updateLac(lac);

                Entry entryRead = new Entry(
                        body.get(Fields.L.LEDGER_ID).asLong(),
                        body.get(Fields.L.ENTRY_ID).asLong(),
                        body.get(Fields.L.LAC).asLong(),
                        body.path(Fields.L.VALUE).asText());

                /*
                    we return the entry as the one with the highest lac. We overwrite the
                    intermediate entry if:
                    - this is the first entry response
                    - the incoming entry has a higher lac than the current intermediate entry
                 */
                if (intermediateEntry == null
                        || entryRead.getLac() > intermediateEntry.getLac()) {
                    intermediateEntry = entryRead;
                }
                positives++;
            } else if (rc.equals(ReturnCodes.Bookie.NO_SUCH_LEDGER) ||
                    rc.equals(ReturnCodes.Bookie.NO_SUCH_ENTRY)) {
                negatives++;
            } else {
                unknowns++;
            }
        }

        public boolean isPositive() {
            return positives >= positiveThreshold;
        }

        public boolean isNegative() {
            return negatives >= negativeThreshold;
        }

        public boolean isUnknown() {
            return unknowns >= unknownThreshold
                    || (noPendingResponses()
                    && !isPositive()
                    && !isNegative());
        }

        public boolean noPendingResponses() {
            return (positives + negatives + unknowns) == writeQuorum;
        }

        public Entry getEntry() {
            return intermediateEntry;
        }
    }

    private static class LongPollDecider extends Decider {
        long previousLac;

        public LongPollDecider(LedgerReadHandle lh,
                               int writeQuorum,
                               int negativeThreshold,
                               int unknownThreshold,
                               long previousLac) {
            super(lh, writeQuorum, Integer.MAX_VALUE, negativeThreshold, unknownThreshold);
            this.previousLac = previousLac;
        }

        @Override
        public boolean isPositive() {
            return (lh.getLastAddConfirmed() > previousLac)
                    || (noPendingResponses()
                    && unknowns < unknownThreshold
                    && negatives < negativeThreshold);
        }
    }
}
