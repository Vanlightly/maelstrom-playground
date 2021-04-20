package com.vanlightly.bookkeeper.kv.bkclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.LedgerStatus;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.Futures;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class LedgerHandle {
    private ObjectMapper mapper;
    private Logger logger;
    private MessageSender messageSender;
    private AtomicBoolean isCancelled;

    Versioned<LedgerMetadata> versionedMetadata;
    Queue<PendingAddOp> pendingAddOps;
    Map<Integer, String> delayedWriteFailedBookies;
    long pendingAddsSequenceHead;
    long lastAddPushed;
    long lastAddConfirmed;
    boolean changingEnsemble;
    LedgerManager ledgerManager;

    boolean pendingClose;
    List<CompletableFuture<Versioned<LedgerMetadata>>> closeFutures;

    public LedgerHandle(ObjectMapper mapper,
                        LedgerManager ledgerManager,
                        MessageSender messageSender,
                        Logger logger,
                        AtomicBoolean isCancelled,
                        Versioned<LedgerMetadata> versionedMetadata) {
        this.mapper = mapper;
        this.ledgerManager = ledgerManager;
        this.messageSender = messageSender;
        this.logger = logger;
        this.isCancelled = isCancelled;
        this.versionedMetadata = versionedMetadata;
        this.pendingAddOps = new ArrayDeque<>();
        this.pendingAddsSequenceHead = -1L;

        if (versionedMetadata.getValue().getStatus() == LedgerStatus.CLOSED) {
            this.lastAddConfirmed = this.lastAddPushed = versionedMetadata.getValue().getLastEntryId();
        } else {
            this.lastAddConfirmed = -1L;
            this.lastAddPushed = -1L;
        }
        this.delayedWriteFailedBookies = new HashMap<>();
        this.closeFutures = new ArrayList<>();
    }

    public void printState() {
        logger.logInfo("------------ Ledger Handle State -------------");
        logger.logInfo("Ledger metadata version: " + versionedMetadata.getVersion());
        logger.logInfo("Ledger metadata: " + versionedMetadata.getValue());
        logger.logInfo("pendingAddsSequenceHead: "+ pendingAddsSequenceHead);
        logger.logInfo("lastAddPushed: "+ lastAddPushed);
        logger.logInfo("lastAddConfirmed: "+ lastAddConfirmed);
        logger.logInfo("changingEnsemble: "+ changingEnsemble);
        logger.logInfo("----------------------------------------------");
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

    public long getLastAddPushed() {
        return lastAddPushed;
    }

    public void setLastAddPushed(long lastAddPushed) {
        this.lastAddPushed = lastAddPushed;
    }

    public void setLastAddConfirmed(long lastAddConfirmed) {
        this.lastAddConfirmed = lastAddConfirmed;
    }

    public CompletableFuture<Entry> addEntry(String value) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        lastAddPushed++;
        Entry entry = new Entry(versionedMetadata.getValue().getLedgerId(), lastAddPushed, value);
        PendingAddOp addOp = new PendingAddOp(mapper,
                messageSender,
                logger,
                entry,
                versionedMetadata.getValue().getCurrentEnsemble(),
                versionedMetadata.getValue().getWriteQuorum(),
                versionedMetadata.getValue().getAckQuorum(),
                this,
                future,
                isCancelled);

        addOp.begin();
        pendingAddOps.add(addOp);

        return future;
    }

    public CompletableFuture<Result<Entry>> read(long entryId) {
        return sequentialRead(entryId, 0, false, 0);
    }

    public CompletableFuture<Result<Entry>> recoveryRead(long entryId) {
        return sequentialRead(entryId, 0, true, 0);
    }

    /*
        Performs a read against a single bookie. If a non-success response is received, it
        sequentially tries the next bookie until either a success response or there are no
        more bookies left to try
     */
    private CompletableFuture<Result<Entry>> sequentialRead(long entryId,
                                                            int bookieIndex,
                                                            boolean isRecoveryRead,
                                                            final int unknown) {
        ObjectNode readReq = mapper.createObjectNode();
        readReq.put(Fields.L.LEDGER_ID, versionedMetadata.getValue().getLedgerId());
        readReq.put(Fields.L.ENTRY_ID, entryId);
        readReq.put(Fields.L.RECOVERY, isRecoveryRead);
        readReq.put(Fields.L.FENCE, isRecoveryRead);
        String bookieId = versionedMetadata.getValue().getCurrentEnsemble().get(bookieIndex);

        return messageSender.sendRequest(bookieId, Commands.Bookie.READ_ENTRY, readReq)
                .thenApply(this::checkForCancellation)
                .thenCompose((JsonNode reply) -> {
                    JsonNode body = reply.get(Fields.BODY);
                    String code = body.get(Fields.RC).asText();
                    if (code.equals(ReturnCodes.OK)) {
                            long lac = body.get(Fields.L.LAC).asLong();

                            if (!isRecoveryRead) {
                                this.updateLac(lac);
                            }

                            Entry entry = new Entry(
                                    body.get(Fields.L.LEDGER_ID).asLong(),
                                    body.get(Fields.L.ENTRY_ID).asLong(),
                                    body.get(Fields.L.LAC).asLong(),
                                    body.get(Fields.L.VALUE).asText());
                            return CompletableFuture.completedFuture(new Result<>(ReturnCodes.OK, entry));
                    } else {
                        int unknown1 = unknown;
                        if (!code.equals(ReturnCodes.Bookie.NO_SUCH_ENTRY)
                                && !code.equals(ReturnCodes.Bookie.NO_SUCH_LEDGER)) {
                            unknown1++;
                        }

                        int nextBookieIndex = bookieIndex + 1;
                        if (nextBookieIndex >= versionedMetadata.getValue().getWriteQuorum()) {
                            if (unknown1 >= 0) {
                                return CompletableFuture.completedFuture(new Result<>(ReturnCodes.Ledger.UNKNOWN, null));
                            } else {
                                return CompletableFuture.completedFuture(new Result<>(code, null));
                            }
                        } else {
                            return sequentialRead(entryId, nextBookieIndex, isRecoveryRead, unknown1);
                        }
                    }
                });
    }

    public CompletableFuture<Result<Entry>> readLac() {
        return parallelRead(-1L, Commands.Bookie.READ_LAC, true);
    }

    public CompletableFuture<Result<Entry>> lacLongPollRead() {
        return parallelRead(lastAddConfirmed, Commands.Bookie.READ_LAC_LONG_POLL, false);
    }

    /*
        Sends a read all the whole ensemble in parallel and returns the entry with the highest LAC
        as long as enough bookies (AckQuorum) respond positively.
     */
    public CompletableFuture<Result<Entry>> parallelRead(long entryId, String readCommand,
                                                         boolean requiresQuorum) {
        CompletableFuture<Result<Entry>> readFuture = new CompletableFuture<>();

        ObjectNode readReq = mapper.createObjectNode();
        readReq.put(Fields.L.LEDGER_ID, versionedMetadata.getValue().getLedgerId());

        int msgTimeout = Constants.Timeouts.TimeoutMs;
        if (readCommand.equals(Commands.Bookie.READ_LAC_LONG_POLL)) {
            readReq.put(Fields.L.PREVIOUS_LAC, entryId);
            readReq.put(Fields.L.LONG_POLL_TIMEOUT_MS, Constants.KvStore.LongPollTimeoutMs);
            msgTimeout = Constants.KvStore.LongPollResponseTimeoutMs;
        } else {
            readReq.put(Fields.L.ENTRY_ID, entryId);
        }

        CompletableFuture<Result<Entry>>[] futures = new CompletableFuture[versionedMetadata.getValue().getWriteQuorum()];

        List<String> bookies = versionedMetadata.getValue().getCurrentEnsemble();
        int writeQuorum = versionedMetadata.getValue().getWriteQuorum();
        for (int b=0; b<writeQuorum; b++) {
            String bookieId = bookies.get(b);
            CompletableFuture<Result<Entry>> future =
                messageSender.sendRequest(bookieId, readCommand, readReq, msgTimeout)
                    .thenApply(this::checkForCancellation)
                    .thenApply((JsonNode reply) -> {
                        JsonNode body = reply.get(Fields.BODY);
                        String code = body.get(Fields.RC).asText();
                        if (code.equals(ReturnCodes.OK)) {
                            long lac = body.get(Fields.L.LAC).asLong();
                            this.updateLac(lac);

                            Entry entry = new Entry(
                                    body.get(Fields.L.LEDGER_ID).asLong(),
                                    body.get(Fields.L.ENTRY_ID).asLong(),
                                    body.get(Fields.L.LAC).asLong(),
                                    body.path(Fields.L.VALUE).asText());
                            return new Result<>(code, entry);
                        } else {
                            return new Result<>(code, null);
                        }
                    });
            futures[b] = future;
        }

        CompletableFuture<Void> allWithFailFast = CompletableFuture.allOf(futures);
        Arrays.stream(futures)
                .forEach(f -> f.exceptionally(e -> {
                    allWithFailFast.completeExceptionally(e);
                    return null;
                }));

        allWithFailFast.
            whenComplete((Void v, Throwable t) -> {
                if (t != null) {
                    readFuture.completeExceptionally(t);
                    return;
                }

                int unknown = 0;
                int positive = 0;
                int negative = 0;
                Entry entry = null;

                for (int b=0; b<writeQuorum; b++) {
                    CompletableFuture<Result<Entry>> future = futures[b];
                    try {
                        Result<Entry> result = future.get();
                        if (result.getCode().equals(ReturnCodes.OK)) {
                            // return the entry with the highest LAC
                            if (entry == null) {
                                entry = result.getData();
                            } else if (entry.getLac() < result.getData().getLac()) {
                                entry = result.getData();
                            }
                            positive++;
                        } else if (result.getCode().equals(ReturnCodes.Bookie.NO_SUCH_LEDGER) ||
                                result.getCode().equals(ReturnCodes.Bookie.NO_SUCH_ENTRY)) {
                            negative++;
                        } else {
                            unknown++;
                        }
                    } catch (Throwable t2) {
                        logger.logError("Failed read", t2);
                        unknown++;
                    }
                }

                int required = 1;
                if (requiresQuorum) {
                    required = minForAckQuorum(versionedMetadata.getValue());
                }

                if (positive >= required) {
                    readFuture.complete(new Result<>(ReturnCodes.OK, entry));
                } else if (unknown >= versionedMetadata.getValue().getAckQuorum()) {
                    readFuture.complete(new Result<>(ReturnCodes.Ledger.UNKNOWN, null));
                } else {
                    readFuture.complete(new Result<>(ReturnCodes.Ledger.NO_QUORUM, null));
                }
            });

        return readFuture;
    }

    private int minForAckQuorum(LedgerMetadata lm) {
        return (lm.getWriteQuorum() - lm.getAckQuorum()) + 1;
    }

    void handleUnrecoverableErrorDuringAdd(String rc) {
        closeInternal(rc);
    }

    void updateLac(long entryId) {
        if (entryId > lastAddConfirmed) {
            lastAddConfirmed = entryId;
        }
    }

    public CompletableFuture<Versioned<LedgerMetadata>> close() {
        if (pendingClose) {
            CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();
            closeFutures.add(future);
            return future;
        }

        return closeInternal(ReturnCodes.Ledger.LEDGER_CLOSED);
    }

    private CompletableFuture<Versioned<LedgerMetadata>> closeInternal(String rc) {
        pendingClose = true;
        versionedMetadata.getValue().setStatus(LedgerStatus.CLOSED);
        versionedMetadata.getValue().setLastEntryId(lastAddConfirmed);
        errorOutPendingAdds(rc);

        return ledgerManager.updateLedgerMetadata(versionedMetadata)
                .thenApply(this::checkForCancellation)
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                    if (t != null) {
                        logger.logDebug("Ledger Close failed: " + t);
                    } else {
                        logger.logDebug("Ledger Close completed");
                    }


                    for (CompletableFuture<Versioned<LedgerMetadata>> future : closeFutures) {
                        if (t != null) {
                            future.completeExceptionally(t);
                        } else {
                            future.complete(vlm);
                        }
                    }
                });
    }

    private void errorOutPendingAdds(String rc) {
        errorOutPendingAdds(rc, drainPendingAdds());
    }

    private List<PendingAddOp> drainPendingAdds() {
        PendingAddOp pendingAddOp;
        List<PendingAddOp> opsDrained = new ArrayList<>(pendingAddOps.size());
        while ((pendingAddOp = pendingAddOps.poll()) != null) {
            opsDrained.add(pendingAddOp);
        }
        return opsDrained;
    }

    private void errorOutPendingAdds(String rc, List<PendingAddOp> ops) {
        logger.logDebug("Erroring " + ops.size() + " pending adds with code: " + rc);
        for (PendingAddOp op : ops) {
            op.completeCallerFuture(rc);
        }
    }

    void sendAddSuccessCallbacks() {
        PendingAddOp pendingAddOp;

        while ((pendingAddOp = pendingAddOps.peek()) != null
                && !changingEnsemble) {
            if (!pendingAddOp.isCommitted()) {
                return;
            }

            pendingAddOps.remove(pendingAddOp);
            pendingAddsSequenceHead = pendingAddOp.getEntry().getEntryId();
            lastAddConfirmed = pendingAddsSequenceHead;
            pendingAddOp.completeCallerFuture(ReturnCodes.OK);
        }
    }

    void handleBookieFailure(Map<Integer, String> failedBookies) {
        // the failed bookie may relate to a committed entry that no longer
        // has a PendingAddOp and the bookie may already have been replaced by
        // a prior ensemble change. We need to filter those out.
        Map<Integer, String> ofCurrentEnsemble = new HashMap<>();
        for (Map.Entry<Integer, String> b : failedBookies.entrySet()) {
            if (versionedMetadata.getValue().getCurrentEnsemble().contains(b.getValue())) {
                ofCurrentEnsemble.put(b.getKey(), b.getValue());
            }
        }

        if (!ofCurrentEnsemble.isEmpty()) {
            if (changingEnsemble) {
                delayedWriteFailedBookies.putAll(ofCurrentEnsemble);
            } else {
                changeEnsemble(ofCurrentEnsemble);
            }
        }
    }

    private void changeEnsemble(Map<Integer, String> failedBookies) {
        logger.logDebug("Changing the ensemble due to failure in bookies: " + failedBookies);
        changingEnsemble = true;

        // work on a copy and replace it at the end
        Versioned<LedgerMetadata> copyOfMetadata = new Versioned<>(
                new LedgerMetadata(versionedMetadata.getValue()), versionedMetadata.getVersion());

        if (copyOfMetadata.getValue().getStatus() == LedgerStatus.CLOSED) {
            logger.logDebug("Ensemble changed cancelled - ledger already closed");
            return;
        } else {
            Map<Integer, String> bookiesToReplace = new HashMap<>(delayedWriteFailedBookies);
            bookiesToReplace.putAll(failedBookies);
            delayedWriteFailedBookies.clear();

            ledgerManager.getAvailableBookies()
                .thenApply(this::checkForCancellation)
                .thenCompose((List<String> availableBookies) -> {
                    logger.logDebug("Available bookies: " + availableBookies.size());
                    availableBookies.removeAll(copyOfMetadata.getValue().getCurrentEnsemble());
                    if (availableBookies.size() < bookiesToReplace.size()) {
                        logger.logError("Couldn't add a new ensemble, not enough bookies");
                        return closeInternal(ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES);
                    } else {
                        logger.logDebug("Enough available bookies");
                        Collections.shuffle(availableBookies);
                        Set<Integer> replacedBookieIndices = new HashSet<>();

                        List<String> newEnsemble = new ArrayList<>(copyOfMetadata.getValue().getCurrentEnsemble());
                        for (int bookieIndex : bookiesToReplace.keySet()) {
                            String newBookie = availableBookies.get(0);
                            newEnsemble.set(bookieIndex, newBookie);
                            replacedBookieIndices.add(bookieIndex);
                        }

                        copyOfMetadata.getValue().replaceCurrentEnsemble(newEnsemble);

                        logger.logDebug("Updating metadata with new ensemble");
                        return ledgerManager.updateLedgerMetadata(copyOfMetadata)
                                .thenApply((Versioned<LedgerMetadata> vlm) -> {
                                    logger.logDebug("Metadata updated with new ensemble. From: "
                                            + versionedMetadata.getValue().getCurrentEnsemble()
                                            + " to: " + vlm.getValue().getCurrentEnsemble());
                                    versionedMetadata = vlm;
                                    unsetSuccessAndSendWriteRequest(newEnsemble, replacedBookieIndices);

                                    return versionedMetadata;
                                });
                    }
                })
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                    changingEnsemble = false;

                    if (t != null) {
                        ensembleChangeFailed(t);
                    } else if (!delayedWriteFailedBookies.isEmpty()) {
                        logger.logInfo("More failed bookies during last ensemble change. Triggered new ensemble change.");
                        changeEnsemble(delayedWriteFailedBookies);
                    } else {
                        logger.logDebug("Ensemble change complete");
                    }
                });
        }
    }

    private void ensembleChangeFailed(Throwable t) {
        if (Futures.unwrap(t) instanceof MetadataException) {
            MetadataException me = (MetadataException) Futures.unwrap(t);
            errorOutPendingAdds(me.getCode());
            logger.logError("The ensemble change has failed due to a metadata error", t);
        } else if (Futures.unwrap(t) instanceof OperationCancelledException) {
            logger.logInfo("The ensemble change has been cancelled");
        } else {
            errorOutPendingAdds(ReturnCodes.UNEXPECTED_ERROR);
            logger.logError("The ensemble change has failed due to an unexpected error", t);
        }
    }

    void unsetSuccessAndSendWriteRequest(List<String> ensemble, final Set<Integer> replacedBookieIndices) {
        for (PendingAddOp pendingAddOp : pendingAddOps) {
            for (Integer bookieIndex : replacedBookieIndices) {
                pendingAddOp.unsetSuccessAndSendWriteRequest(ensemble, bookieIndex);
            }
        }
    }

    private <T> T checkForCancellation(T t) {
        if (isCancelled.get()) {
            throw new OperationCancelledException();
        }

        return t;
    }
}
