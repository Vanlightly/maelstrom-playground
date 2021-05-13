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

import static com.vanlightly.bookkeeper.util.Futures.Delay;

public class LedgerReadHandle {
    private Logger logger = LogManager.getLogger(this.getClass().getSimpleName());
    private ObjectMapper mapper = MsgMapping.getMapper();
    private MessageSender messageSender;
    private AtomicBoolean isCancelled;

    Versioned<LedgerMetadata> versionedMetadata;
    long lastAddConfirmed;
    LedgerManager ledgerManager;
    Queue<PendingRead> pendingReads;

    public LedgerReadHandle(LedgerManager ledgerManager,
                            MessageSender messageSender,
                            Versioned<LedgerMetadata> versionedMetadata) {
        this.ledgerManager = ledgerManager;
        this.messageSender = messageSender;
        // must be a copy as sharing between handles during recovery can cause inconsistency
        this.versionedMetadata = new Versioned<>(versionedMetadata.getValue(),
                versionedMetadata.getVersion());
        this.isCancelled = new AtomicBoolean();
        this.pendingReads = new ArrayDeque<>();

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
                + "lastAddConfirmed: " + lastAddConfirmed + System.lineSeparator()
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

    /*
        Performs a read of an entry to a single bookie at a time. If the response is
        not positive, it goes to the next until it succeeds or runs out of bookies.
        Supports pipelining of reads.
     */
    public CompletableFuture<Result<Entry>> read(long entryId) {
        CompletableFuture<Result<Entry>> future = new CompletableFuture<>();

        PendingRead pendingRead = new PendingRead(entryId,
                future,
                lm().getWriteQuorum(),
                1,
                lm().getWriteQuorum(),
                lm().getWriteQuorum());

        pendingReads.add(pendingRead);
        // TODO: implement sticky bookie reads optimization
        sequentialRead(entryId, 0, pendingRead);

        return future;
    }

    /*
        Performs a read against a single bookie. If a non-success response is received, it
        sequentially tries the next bookie until either a success response or there are no
        more bookies left to try
     */
    private void sequentialRead(long entryId,
                                int bookieIndex,
                                PendingRead pendingRead) {
        ObjectNode readReq = mapper.createObjectNode();
        readReq.put(Fields.L.LEDGER_ID, lm().getLedgerId());
        readReq.put(Fields.L.ENTRY_ID, entryId);
        String bookieId = lm().getEnsembleFor(entryId).get(bookieIndex);

        messageSender.sendRequest(bookieId, Commands.Bookie.READ_ENTRY, readReq)
            .thenApply(this::checkForCancellation)
            .thenAccept((JsonNode reply) -> {
                JsonNode body = reply.get(Fields.BODY);
                String rc = body.get(Fields.RC).asText();
                pendingRead.register(rc, body);

                if (!pendingRead.isPositive()) {
                    int nextBookieIndex = bookieIndex + 1;
                    if (nextBookieIndex < lm().getWriteQuorum()) {
                        sequentialRead(entryId, nextBookieIndex, pendingRead);
                    }
                }

                completeReadFutures();
            })
            .whenComplete((Void v, Throwable t) -> {
                if (isError(t)) {
                    logger.logError("Failed performing read of entry " + entryId, t);
                    pendingRead.register(ReturnCodes.UNEXPECTED_ERROR, null);
                }
            });
    }

    public CompletableFuture<Result<Entry>> recoveryRead(long entryId) {
        CompletableFuture<Result<Entry>> future = new CompletableFuture<>();

        // a recovery read only needs an AQ to succeed, but a negative
        // requires QC as this then precludes the possibility of a success
        logger.logDebug("Sending recovery read requests to: " + lm().getEnsembleFor(entryId));
        PendingRead recoveryPendingRead = new PendingRead(entryId,
                future,
                lm().getWriteQuorum(),
                lm().getAckQuorum(),  // positive threshold
                quorumCoverage(lm()), // negative threshold
                quorumCoverage(lm())); // unknown threshold
        pendingReads.add(recoveryPendingRead);

        parallelRead(entryId, Commands.Bookie.READ_ENTRY,
                Config.RecoveryReadsFence,
                recoveryPendingRead);

        return future;
    }

    public CompletableFuture<Result<Entry>> readLacWithFencing() {
        CompletableFuture<Result<Entry>> future = new CompletableFuture<>();

        logger.logDebug("Sending fencing requests to: " + lm().getCurrentEnsemble());
        // for fencing LAC to be complete we must ensure that no AQ of bookies
        // remains unfenced, hence the QC threshold for positive and AQ for unknown
        PendingRead fencingPendingRead = new PendingRead(-1L,
                future,
                lm().getWriteQuorum(),
                quorumCoverage(lm()),  // positive threshold
                lm().getWriteQuorum(), // negative threshold (not possible with fencing LAC read)
                lm().getAckQuorum());   // unknown threshold

        pendingReads.add(fencingPendingRead);

        parallelRead(-1L, Commands.Bookie.READ_LAC, true, fencingPendingRead);

        return future;
    }

    public CompletableFuture<Result<Entry>> lacLongPollRead() {
        CompletableFuture<Result<Entry>> future = new CompletableFuture<Result<Entry>>();
        PendingRead lpPendingRead = new LongPollPendingRead(
                future,
                lm().getWriteQuorum(),
                quorumCoverage(lm()),
                quorumCoverage(lm()),
                lastAddConfirmed);
        pendingReads.add(lpPendingRead);

        parallelRead(lastAddConfirmed, Commands.Bookie.READ_LAC_LONG_POLL,
                false, lpPendingRead);

        return future;
    }

    /*
        Sends a read all the whole ensemble in parallel and returns the entry with the highest LAC
        as long as enough bookies respond positively.
        Does not support pipelining at this point.
     */
    public void parallelRead(long entryId,
                             String readCommand,
                             boolean fence,
                             PendingRead pendingRead) {
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

        for (int b = 0; b < writeQuorum; b++) {
            int i = b;
            if (readCommand.equals(Commands.Bookie.READ_LAC)
                    && fence
                    && b == writeQuorum - 1
                    && Config.LoseFencingMsg) {
                // TODO: REMOVE => lose a single fencing LAC read
                i = 0;
            }

            // TODO: REMOVE => use delays to increase probability of read/write overlap
            int delay = (readCommand.equals(Commands.Bookie.READ_ENTRY)
                    ? Config.AddReadSpreadMs
                    : 0) * i;

            int finalI = i;
            int finalMsgTimeout = msgTimeout;
            Delay.apply(delay).thenRun(() -> {
                String bookieId = bookies.get(finalI);

                messageSender.sendRequest(bookieId, readCommand, readReq, finalMsgTimeout)
                        .thenApply(this::checkForCancellation)
                        .thenAccept((JsonNode reply) -> {
                            JsonNode body = reply.get(Fields.BODY);
                            String rc = body.get(Fields.RC).asText();
                            pendingRead.register(rc, body);

                            completeReadFutures();
                        })
                        .whenComplete((Void v, Throwable t) -> {
                            if (t != null) {
                                if (isError(t)) {
                                    // if it was cancelled, don't log it
                                    logger.logError("LedgerHandle: Failed performing parallel read", t);
                                }
                                if (!pendingRead.getFuture().isDone()) {
                                    pendingRead.getFuture().completeExceptionally(Futures.unwrap(t));
                                }
                            }
                        });
            });
        }
    }

    private void completeReadFutures() {
        PendingRead pendingRead;

        while ((pendingRead = pendingReads.peek()) != null) {
            if (pendingRead.isPositive()) {
                updateLac(pendingRead.getEntry().getLac());
                pendingRead.getFuture().complete(new Result<>(ReturnCodes.OK, pendingRead.getEntry()));
                pendingReads.poll();
            } else if (pendingRead.isNegative()) {
                logger.logDebug("LedgerHandle: Parallel read is negative for entry: " + pendingRead.getEntryId()
                        + " with negatives=" + pendingRead.negatives);
                pendingRead.getFuture().complete(new Result<>(ReturnCodes.Ledger.NO_QUORUM, null));
                pendingReads.poll();
            } else if (pendingRead.isUnknown() || pendingRead.noPendingResponses()) {
                pendingRead.getFuture().complete(new Result<>(ReturnCodes.Ledger.UNKNOWN, null));
                pendingReads.poll();
            } else {
                // not enough responses yet
                break;
            }
        }
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

    private boolean isError(Throwable t) {
        return t != null && !(t instanceof OperationCancelledException)
                && !(Futures.unwrap(t) instanceof OperationCancelledException);
    }

    private static class PendingRead {

        int unknowns;
        int positives;
        int negatives;
        Entry intermediateEntry;

        long entryId;
        CompletableFuture<Result<Entry>> future;
        int writeQuorum;
        int positiveThreshold;
        int negativeThreshold;
        int unknownThreshold;

        public PendingRead(long entryId,
                           CompletableFuture<Result<Entry>> future,
                           int writeQuorum,
                           int positiveThreshold,
                           int negativeThreshold,
                           int unknownThreshold) {
            this.entryId = entryId;
            this.future = future;
            this.writeQuorum = writeQuorum;
            this.positiveThreshold = positiveThreshold;
            this.negativeThreshold = negativeThreshold;
            this.unknownThreshold = unknownThreshold;
        }

        public void register(String rc, JsonNode body) {
            if (rc.equals(ReturnCodes.OK)) {
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

        public long getEntryId() {
            return entryId;
        }

        public CompletableFuture<Result<Entry>> getFuture() {
            return future;
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

    private static class LongPollPendingRead extends PendingRead {
        long previousLac;

        public LongPollPendingRead(CompletableFuture<Result<Entry>> future,
                                   int writeQuorum,
                                   int negativeThreshold,
                                   int unknownThreshold,
                                   long previousLac) {
            super(-1L, future, writeQuorum, Integer.MAX_VALUE,
                    negativeThreshold, unknownThreshold);
            this.previousLac = previousLac;
        }

        @Override
        public boolean isPositive() {
            return intermediateEntry != null
                    && (intermediateEntry.getEntryId() > previousLac
                        || (noPendingResponses()
                            && unknowns < unknownThreshold
                            && negatives < negativeThreshold));
        }
    }
}
