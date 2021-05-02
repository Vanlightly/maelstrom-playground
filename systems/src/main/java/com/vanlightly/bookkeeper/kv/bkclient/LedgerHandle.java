//package com.vanlightly.bookkeeper.kv.bkclient;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import com.vanlightly.bookkeeper.*;
//import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
//import com.vanlightly.bookkeeper.metadata.LedgerStatus;
//import com.vanlightly.bookkeeper.metadata.Versioned;
//import com.vanlightly.bookkeeper.util.Futures;
//import com.vanlightly.bookkeeper.util.InvariantViolationException;
//
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//public class LedgerHandle {
//    private ObjectMapper mapper;
//    private Logger logger;
//    private MessageSender messageSender;
//    private AtomicBoolean isCancelled;
//
//    Versioned<LedgerMetadata> versionedMetadata;
//    Queue<PendingAddOp> pendingAddOps;
//    Map<Integer, String> delayedWriteFailedBookies;
//    long pendingAddsSequenceHead;
//    long lastAddPushed;
//    long lastAddConfirmed;
//    boolean changingEnsemble;
//    LedgerManager ledgerManager;
//
//    boolean pendingClose;
//    List<CompletableFuture<Versioned<LedgerMetadata>>> closeFutures;
//
//    public LedgerHandle(ObjectMapper mapper,
//                        LedgerManager ledgerManager,
//                        MessageSender messageSender,
//                        Logger logger,
//                        Versioned<LedgerMetadata> versionedMetadata) {
//        this.mapper = mapper;
//        this.ledgerManager = ledgerManager;
//        this.messageSender = messageSender;
//        this.logger = logger;
//        this.versionedMetadata = versionedMetadata;
//        this.pendingAddOps = new ArrayDeque<>();
//        this.pendingAddsSequenceHead = -1L;
//        this.isCancelled = new AtomicBoolean();
//
//        if (versionedMetadata.getValue().getStatus() == LedgerStatus.CLOSED) {
//            this.lastAddConfirmed = this.lastAddPushed = versionedMetadata.getValue().getLastEntryId();
//        } else {
//            this.lastAddConfirmed = -1L;
//            this.lastAddPushed = -1L;
//        }
//        this.delayedWriteFailedBookies = new HashMap<>();
//        this.closeFutures = new ArrayList<>();
//    }
//
//    public void cancel() {
//        isCancelled.set(true);
//    }
//
//    public void printState() {
//        logger.logInfo("------------ Ledger Handle State -------------");
//        logger.logInfo("Ledger metadata version: " + versionedMetadata.getVersion());
//        logger.logInfo("Ledger metadata: " + versionedMetadata.getValue());
//        logger.logInfo("pendingAddsSequenceHead: "+ pendingAddsSequenceHead);
//        logger.logInfo("lastAddPushed: "+ lastAddPushed);
//        logger.logInfo("lastAddConfirmed: "+ lastAddConfirmed);
//        logger.logInfo("changingEnsemble: "+ changingEnsemble);
//        logger.logInfo("----------------------------------------------");
//    }
//
//    public Versioned<LedgerMetadata> getCachedLedgerMetadata() {
//        return versionedMetadata;
//    }
//
//    public void setCachedLedgerMetadata(Versioned<LedgerMetadata> updatedMetadata) {
//        versionedMetadata = updatedMetadata;
//    }
//
//    public long getLedgerId() {
//        return versionedMetadata.getValue().getLedgerId();
//    }
//
//    public long getLastAddConfirmed() {
//        return lastAddConfirmed;
//    }
//
//    public long getLastAddPushed() {
//        return lastAddPushed;
//    }
//
//    public void setLastAddPushed(long lastAddPushed) {
//        this.lastAddPushed = lastAddPushed;
//    }
//
//    public void setLastAddConfirmed(long lastAddConfirmed) {
//        this.lastAddConfirmed = lastAddConfirmed;
//    }
//
//    public CompletableFuture<Entry> addEntry(String value) {
//        return addEntry(value, false);
//    }
//
//    public CompletableFuture<Entry> recoveryAddEntry(String value) {
//        return addEntry(value, true);
//    }
//
//    private CompletableFuture<Entry> addEntry(String value, boolean isRecoveryAdd) {
//        CompletableFuture<Entry> future = new CompletableFuture<>();
//
//        if (value == "") {
//            throw new InvariantViolationException("Empty value!");
//        }
//
//        lastAddPushed++;
//        Entry entry = new Entry(lm().getLedgerId(), lastAddPushed, value);
//        PendingAddOp addOp = new PendingAddOp(mapper,
//                messageSender,
//                logger,
//                entry,
//                lm().getCurrentEnsemble(),
//                lm().getWriteQuorum(),
//                lm().getAckQuorum(),
//                this,
//                isRecoveryAdd,
//                future,
//                isCancelled);
//
//        addOp.begin();
//        pendingAddOps.add(addOp);
//
//        return future;
//    }
//
//    public CompletableFuture<Result<Entry>> read(long entryId) {
//        Decider readDecider = new Decider(this,
//                lm().getWriteQuorum(),
//                1,
//                lm().getWriteQuorum(),
//                lm().getWriteQuorum());
//        // TODO: implement sticky bookie reads optimization
//        return sequentialRead(entryId, 0, readDecider);
//    }
//
//    /*
//        Performs a read against a single bookie. If a non-success response is received, it
//        sequentially tries the next bookie until either a success response or there are no
//        more bookies left to try
//     */
//    private CompletableFuture<Result<Entry>> sequentialRead(long entryId,
//                                                            int bookieIndex,
//                                                            Decider decider) {
//        ObjectNode readReq = mapper.createObjectNode();
//        readReq.put(Fields.L.LEDGER_ID, lm().getLedgerId());
//        readReq.put(Fields.L.ENTRY_ID, entryId);
//        String bookieId = lm().getCurrentEnsemble().get(bookieIndex);
//
//        return messageSender.sendRequest(bookieId, Commands.Bookie.READ_ENTRY, readReq)
//                .thenApply(this::checkForCancellation)
//                .thenCompose((JsonNode reply) -> {
//                    JsonNode body = reply.get(Fields.BODY);
//                    String rc = body.get(Fields.RC).asText();
//                    decider.register(rc, body, false);
//
//                    if (decider.isPositive()) {
//                        return CompletableFuture.completedFuture(new Result<>(ReturnCodes.OK, decider.getEntry()));
//                    } else {
//                        int nextBookieIndex = bookieIndex + 1;
//                        if (nextBookieIndex >= lm().getWriteQuorum()) {
//                            if (decider.isNegative()) {
//                                return CompletableFuture.completedFuture(new Result<>(rc, null));
//                            } else {
//                                return CompletableFuture.completedFuture(new Result<>(ReturnCodes.Ledger.UNKNOWN, null));
//                            }
//                        } else {
//                            return sequentialRead(entryId, nextBookieIndex, decider);
//                        }
//                    }
//                });
//    }
//
//    public CompletableFuture<Result<Entry>> recoveryRead(long entryId) {
//        // a recovery read only needs an AQ to succeed, but a negative
//        // requires QC as this then precludes the possibility of a success
//        Decider recoveryReadDecider = new Decider(this,
//                lm().getWriteQuorum(),
//                lm().getAckQuorum(),   // positive threshold
//                quorumCoverage(lm()),  // negative threshold
//                quorumCoverage(lm())); // unknown threshold
//        return parallelRead(entryId, Commands.Bookie.READ_ENTRY, true, true, recoveryReadDecider);
//    }
//
//    public CompletableFuture<Result<Entry>> readLacWithFencing() {
//        // for fencing LAC to be complete we must ensure that no AQ of bookies
//        // remains unfenced, hence the QC threshold for positive and AQ for unknown
//        Decider fencingDecider = new Decider(this,
//                lm().getWriteQuorum(),
//                quorumCoverage(lm()),  // positive threshold
//                lm().getWriteQuorum(), // negative threshold (not possible with fencing LAC read)
//                lm().getAckQuorum());  // unknown threshold
//        return parallelRead(-1L, Commands.Bookie.READ_LAC, true, false, fencingDecider);
//    }
//
//    public CompletableFuture<Result<Entry>> lacLongPollRead() {
//        Decider lpDecider = new LongPollDecider(this,
//                lm().getWriteQuorum(),
//                quorumCoverage(lm()),
//                quorumCoverage(lm()),
//                lastAddConfirmed);
//        return parallelRead(lastAddConfirmed, Commands.Bookie.READ_LAC_LONG_POLL,
//                false, false, lpDecider);
//    }
//
//    /*
//        Sends a read all the whole ensemble in parallel and returns the entry with the highest LAC
//        as long as enough bookies respond positively.
//     */
//    public CompletableFuture<Result<Entry>> parallelRead(long entryId,
//                                                         String readCommand,
//                                                         boolean fence,
//                                                         boolean isRecovery,
//                                                         Decider decider) {
//        CompletableFuture<Result<Entry>> readFuture = new CompletableFuture<>();
//
//        ObjectNode readReq = mapper.createObjectNode();
//        readReq.put(Fields.L.LEDGER_ID, versionedMetadata.getValue().getLedgerId());
//
//        int msgTimeout = Constants.Timeouts.TimeoutMs;
//        if (readCommand.equals(Commands.Bookie.READ_LAC_LONG_POLL)) {
//            readReq.put(Fields.L.PREVIOUS_LAC, entryId);
//            readReq.put(Fields.L.LONG_POLL_TIMEOUT_MS, Constants.KvStore.LongPollTimeoutMs);
//            msgTimeout = Constants.KvStore.LongPollResponseTimeoutMs;
//        }
//
//        readReq.put(Fields.L.ENTRY_ID, entryId);
//
//        if (fence) {
//            readReq.put(Fields.L.FENCE, true);
//        }
//
//        if (isRecovery) {
//            readReq.put(Fields.L.RECOVERY, true);
//        }
//
//        List<String> bookies = lm().getCurrentEnsemble();
//        int writeQuorum = lm().getWriteQuorum();
//
//        for (int b=0; b<writeQuorum; b++) {
//            String bookieId = bookies.get(b);
//            messageSender.sendRequest(bookieId, readCommand, readReq, msgTimeout)
//                    .thenApply(this::checkForCancellation)
//                    .thenAccept((JsonNode reply) -> {
//                        if (readFuture.isDone()) {
//                            // seen enough responses already
//                            return;
//                        }
//
//                        JsonNode body = reply.get(Fields.BODY);
//                        String rc = body.get(Fields.RC).asText();
//                        decider.register(rc, body, isRecovery);
//
//                        if (decider.isPositive()) {
//                            readFuture.complete(new Result<>(ReturnCodes.OK, decider.getEntry()));
//                        } else if (decider.isNegative()) {
//                            logger.logDebug("LedgerHandle: Parallel read is negative for entry: " + entryId + " with negatives=" + decider.negatives);
//                            readFuture.complete(new Result<>(ReturnCodes.Ledger.NO_QUORUM, null));
//                        } else if (decider.isUnknown()) {
//                            readFuture.complete(new Result<>(ReturnCodes.Ledger.UNKNOWN, null));
//                        }
//                    })
//                .whenComplete((Void v, Throwable t) -> {
//                    if (t != null) {
//                        logger.logError("LedgerHandle: Failed performing parallel read", t);
//                        if (!readFuture.isDone()) {
//                            readFuture.completeExceptionally(Futures.unwrap(t));
//                        }
//                    }
//                });
//
//        }
//
//        return readFuture;
//    }
//
//    private int quorumCoverage(LedgerMetadata lm) {
//        return (lm.getWriteQuorum() - lm.getAckQuorum()) + 1;
//    }
//
//    void handleUnrecoverableErrorDuringAdd(String rc, boolean isRecoveryOp) {
//        if (isRecoveryOp) {
//            errorOutPendingAdds(rc);
//        } else {
//            closeInternal(rc);
//        }
//    }
//
//    void updateLac(long entryId) {
//        if (entryId > lastAddConfirmed) {
//            lastAddConfirmed = entryId;
//        }
//    }
//
//    public CompletableFuture<Versioned<LedgerMetadata>> close() {
//        if (pendingClose) {
//            CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();
//            closeFutures.add(future);
//            return future;
//        }
//
//        return closeInternal(ReturnCodes.Ledger.LEDGER_CLOSED);
//    }
//
//    private CompletableFuture<Versioned<LedgerMetadata>> closeInternal(String rc) {
//        logger.logDebug("LedgerHandle: closing ledger");
//        if (lm().getStatus() == LedgerStatus.CLOSED) {
//            logger.logDebug("Ledger already closed");
//            closeFutures.stream().forEach(f -> f.complete(versionedMetadata));
//            return CompletableFuture.completedFuture(versionedMetadata);
//        }
//
//        pendingClose = true;
//        lm().setStatus(LedgerStatus.CLOSED);
//        lm().setLastEntryId(lastAddConfirmed);
//        errorOutPendingAdds(rc);
//
//        logger.logDebug("LedgerHandle: Sending metadata update with closed status for ledger: " + lm().getLedgerId());
//        return ledgerManager.updateLedgerMetadata(versionedMetadata)
//                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
//                    if (t != null) {
//                        logger.logError("LedgerHandle: Ledger Close failed", t);
//                    } else {
//                        logger.logDebug("LedgerHandle: Ledger Close completed");
//                    }
//
//                    for (CompletableFuture<Versioned<LedgerMetadata>> future : closeFutures) {
//                        if (t != null) {
//                            future.completeExceptionally(t);
//                        } else {
//                            future.complete(vlm);
//                        }
//                    }
//                });
//    }
//
//    private void errorOutPendingAdds(String rc) {
//        errorOutPendingAdds(rc, drainPendingAdds());
//    }
//
//    private List<PendingAddOp> drainPendingAdds() {
//        PendingAddOp pendingAddOp;
//        List<PendingAddOp> opsDrained = new ArrayList<>(pendingAddOps.size());
//        while ((pendingAddOp = pendingAddOps.poll()) != null) {
//            opsDrained.add(pendingAddOp);
//        }
//        return opsDrained;
//    }
//
//    private void errorOutPendingAdds(String rc, List<PendingAddOp> ops) {
//        logger.logDebug("LedgerHandle: Erroring " + ops.size() + " pending adds with code: " + rc);
//        for (PendingAddOp op : ops) {
//            op.completeCallerFuture(rc);
//        }
//    }
//
//    void sendAddSuccessCallbacks() {
//        PendingAddOp pendingAddOp;
//
//        while ((pendingAddOp = pendingAddOps.peek()) != null
//                && !changingEnsemble) {
//            if (!pendingAddOp.isCommitted()) {
//                return;
//            }
//
//            pendingAddOps.remove(pendingAddOp);
//            pendingAddsSequenceHead = pendingAddOp.getEntry().getEntryId();
//            lastAddConfirmed = pendingAddsSequenceHead;
//            pendingAddOp.completeCallerFuture(ReturnCodes.OK);
//        }
//    }
//
//    void handleBookieFailure(Map<Integer, String> failedBookies) {
//        // the failed bookie may relate to a committed entry that no longer
//        // has a PendingAddOp and the bookie may already have been replaced by
//        // a prior ensemble change. We need to filter those out.
//        Map<Integer, String> ofCurrentEnsemble = new HashMap<>();
//        for (Map.Entry<Integer, String> b : failedBookies.entrySet()) {
//            if (versionedMetadata.getValue().getCurrentEnsemble().contains(b.getValue())) {
//                ofCurrentEnsemble.put(b.getKey(), b.getValue());
//            }
//        }
//
//        if (!ofCurrentEnsemble.isEmpty()) {
//            if (changingEnsemble) {
//                delayedWriteFailedBookies.putAll(ofCurrentEnsemble);
//            } else {
//                changeEnsemble(ofCurrentEnsemble);
//            }
//        }
//    }
//
//    private void changeEnsemble(Map<Integer, String> failedBookies) {
//        Map<Integer, String> bookiesToReplace = validFailedBookies(failedBookies);
//        if (bookiesToReplace.isEmpty()) {
//            logger.logDebug("LedgerHandle: Ignoring bookie write failure for bookie that is no longer a member of the current ensemble");
//            return;
//        } else if (lm().getStatus() == LedgerStatus.CLOSED) {
//            logger.logDebug("LedgerHandle: Ensemble changed cancelled - ledger already closed");
//            return;
//        }
//
//        logger.logDebug("LedgerHandle: Changing the ensemble due to failure in bookies: " + bookiesToReplace.values());
//        changingEnsemble = true;
//
//        // work on a copy and replace it at the end
//        Versioned<LedgerMetadata> copyOfMetadata = new Versioned<>(
//                new LedgerMetadata(lm()), versionedMetadata.getVersion());
//
//        ledgerManager.getAvailableBookies()
//            .thenApply(this::checkForCancellation)
//            .thenCompose((List<String> availableBookies) -> {
//                logger.logDebug("LedgerHandle: Available bookies: " + availableBookies);
//                availableBookies.removeAll(copyOfMetadata.getValue().getCurrentEnsemble());
//                logger.logDebug("LedgerHandle: Available bookies not in current ensemble: " + availableBookies);
//                if (availableBookies.size() < bookiesToReplace.size()) {
//                    logger.logError("LedgerHandle: Couldn't add a new ensemble, not enough bookies");
//                    if (lm().getStatus() == LedgerStatus.IN_RECOVERY) {
//                        // don't close if we're in recovery, else we'd truncate data
//                        return Futures.failedFuture(new BkException("Not enough bookies to change the ensemble",
//                                ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES));
//                    } else {
//                        return closeInternal(ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES);
//                    }
//                } else {
//                    logger.logDebug("LedgerHandle: Enough available bookies");
//                    Collections.shuffle(availableBookies);
//                    Set<Integer> replacedBookieIndices = new HashSet<>();
//
//                    List<String> newEnsemble = new ArrayList<>(copyOfMetadata.getValue().getCurrentEnsemble());
//                    int replaceIndex = 0;
//                    for (int bookieIndex : bookiesToReplace.keySet()) {
//                        String newBookie = availableBookies.get(replaceIndex);
//                        newEnsemble.set(bookieIndex, newBookie);
//                        replacedBookieIndices.add(bookieIndex);
//                        replaceIndex++;
//                    }
//
//                    if (lastAddConfirmed + 1 == copyOfMetadata.getValue().getEnsembles().lastKey()) {
//                        logger.logDebug("LedgerHandle: Replacing last ensemble: "
//                                + copyOfMetadata.getValue().getCurrentEnsemble()
//                                + " with new ensemble: " + newEnsemble);
//                        copyOfMetadata.getValue().replaceCurrentEnsemble(newEnsemble);
//                    } else {
//                        logger.logDebug("LedgerHandle: Appending new ensemble: " + newEnsemble);
//                        copyOfMetadata.getValue().addEnsemble(lastAddConfirmed + 1, newEnsemble);
//                    }
//
//                    return ledgerManager.updateLedgerMetadata(copyOfMetadata)
//                            .thenApply(this::checkForCancellation)
//                            .thenApply((Versioned<LedgerMetadata> vlm) -> {
//                                logger.logDebug("LedgerHandle: Metadata updated. Current ensemble: "
//                                        + vlm.getValue().getCurrentEnsemble()
//                                        + " From: " + versionedMetadata.getValue().getCurrentEnsemble());
//                                versionedMetadata = vlm;
//                                unsetSuccessAndSendWriteRequest(newEnsemble, replacedBookieIndices);
//
//                                return versionedMetadata;
//                            });
//                }
//            })
//            .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
//                changingEnsemble = false;
//
//                if (t != null) {
//                    ensembleChangeFailed(t);
//                } else if (!delayedWriteFailedBookies.isEmpty() && delayedWriteFailedBookies.values().stream()
//                            .anyMatch(x -> vlm.getValue().getCurrentEnsemble().contains(x))) {
//                    logger.logInfo("LedgerHandle: More failed bookies during last ensemble change. Triggered new ensemble change.");
//                    changeEnsemble(delayedWriteFailedBookies);
//                } else {
//                    logger.logDebug("LedgerHandle: Ensemble change complete");
//                }
//            });
//    }
//
//    private Map<Integer, String> validFailedBookies(Map<Integer, String> failedBookies) {
//        Map<Integer, String> candidates = new HashMap<>(delayedWriteFailedBookies);
//        candidates.putAll(failedBookies);
//
//        Map<Integer, String> toReplace = new HashMap<>();
//        for (Map.Entry<Integer, String> candidate : candidates.entrySet()) {
//            if (lm().getCurrentEnsemble().contains(candidate.getValue())) {
//                toReplace.put(candidate.getKey(), candidate.getValue());
//            }
//        }
//
//        delayedWriteFailedBookies.clear();
//
//        return toReplace;
//    }
//
//    private void ensembleChangeFailed(Throwable t) {
//        if (Futures.unwrap(t) instanceof MetadataException) {
//            MetadataException me = (MetadataException) Futures.unwrap(t);
//            errorOutPendingAdds(me.getCode());
//            logger.logError("LedgerHandle: The ensemble change has failed due to a metadata error", t);
//        } else if (Futures.unwrap(t) instanceof BkException) {
//            BkException me = (BkException) Futures.unwrap(t);
//            errorOutPendingAdds(me.getCode());
//            logger.logError("LedgerHandle: The ensemble change has failed due to an error.", t);
//        } else if (Futures.unwrap(t) instanceof OperationCancelledException) {
//            logger.logInfo("LedgerHandle: The ensemble change has been cancelled");
//        } else {
//            errorOutPendingAdds(ReturnCodes.UNEXPECTED_ERROR);
//            logger.logError("LedgerHandle: The ensemble change has failed due to an unexpected error", t);
//        }
//    }
//
//    void unsetSuccessAndSendWriteRequest(List<String> ensemble, final Set<Integer> replacedBookieIndices) {
//        for (PendingAddOp pendingAddOp : pendingAddOps) {
//            for (Integer bookieIndex : replacedBookieIndices) {
//                pendingAddOp.unsetSuccessAndSendWriteRequest(ensemble, bookieIndex);
//            }
//        }
//    }
//
//    private LedgerMetadata lm() {
//        return versionedMetadata.getValue();
//    }
//
//    private <T> T checkForCancellation(T t) {
//        if (isCancelled.get()) {
//            throw new OperationCancelledException();
//        }
//
//        return t;
//    }
//
//    private static class Decider {
//        int unknowns;
//        int positives;
//        int negatives;
//        Entry intermediateEntry;
//
//        LedgerHandle lh;
//        int writeQuorum;
//        int positiveThreshold;
//        int negativeThreshold;
//        int unknownThreshold;
//
//        public Decider(LedgerHandle lh,
//                       int writeQuorum,
//                       int positiveThreshold,
//                       int negativeThreshold,
//                       int unknownThreshold) {
//            this.lh = lh;
//            this.writeQuorum = writeQuorum;
//            this.positiveThreshold = positiveThreshold;
//            this.negativeThreshold = negativeThreshold;
//            this.unknownThreshold = unknownThreshold;
//        }
//
//        public void register(String rc, JsonNode body, boolean isRecovery) {
//            if (rc.equals(ReturnCodes.OK)) {
//                long lac = body.get(Fields.L.LAC).asLong();
//                if (isRecovery) {
//                    lh.updateLac(lac);
//                }
//
//                Entry entryRead = new Entry(
//                        body.get(Fields.L.LEDGER_ID).asLong(),
//                        body.get(Fields.L.ENTRY_ID).asLong(),
//                        body.get(Fields.L.LAC).asLong(),
//                        body.path(Fields.L.VALUE).asText());
//
//                /*
//                    we return the entry as the one with the highest lac. We overwrite the
//                    intermediate entry if:
//                    - this is the first entry response
//                    - the incoming entry has a higher lac than the current intermediate entry
//                 */
//                if (intermediateEntry == null
//                        || entryRead.getLac() > intermediateEntry.getLac()) {
//                    intermediateEntry = entryRead;
//                }
//                positives++;
//            } else if (rc.equals(ReturnCodes.Bookie.NO_SUCH_LEDGER) ||
//                    rc.equals(ReturnCodes.Bookie.NO_SUCH_ENTRY)) {
//                negatives++;
//            } else {
//                unknowns++;
//            }
//        }
//
//        public boolean isPositive() {
//            return positives >= positiveThreshold;
//        }
//
//        public boolean isNegative() {
//            return negatives >= negativeThreshold;
//        }
//
//        public boolean isUnknown() {
//            return unknowns >= unknownThreshold
//                    || (noPendingResponses()
//                            && !isPositive()
//                            && !isNegative());
//        }
//
//        public boolean noPendingResponses() {
//            return (positives + negatives + unknowns) == writeQuorum;
//        }
//
//        public Entry getEntry() {
//            return intermediateEntry;
//        }
//    }
//
//    private static class LongPollDecider extends Decider {
//        long previousLac;
//
//        public LongPollDecider(LedgerHandle lh,
//                               int writeQuorum,
//                               int negativeThreshold,
//                               int unknownThreshold,
//                               long previousLac) {
//            super(lh, writeQuorum, Integer.MAX_VALUE, negativeThreshold, unknownThreshold);
//            this.previousLac = previousLac;
//        }
//
//        @Override
//        public boolean isPositive() {
//            return (lh.getLastAddConfirmed() > previousLac)
//                    || (noPendingResponses()
//                        && unknowns < unknownThreshold
//                        && negatives < negativeThreshold);
//        }
//    }
//}
