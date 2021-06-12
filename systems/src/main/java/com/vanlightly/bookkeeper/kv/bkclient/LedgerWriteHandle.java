package com.vanlightly.bookkeeper.kv.bkclient;

import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.MetadataException;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.LedgerStatus;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class LedgerWriteHandle {
    private Logger logger = LogManager.getLogger(this.getClass().getSimpleName());
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

    public LedgerWriteHandle(LedgerManager ledgerManager,
                             MessageSender messageSender,
                             Versioned<LedgerMetadata> versionedMetadata) {
        this.ledgerManager = ledgerManager;
        this.messageSender = messageSender;
        this.versionedMetadata = versionedMetadata;
        this.pendingAddOps = new ArrayDeque<>();
        this.pendingAddsSequenceHead = -1L;
        this.isCancelled = new AtomicBoolean();

        if (versionedMetadata.getValue().getStatus() == LedgerStatus.CLOSED) {
            this.lastAddConfirmed = this.lastAddPushed = versionedMetadata.getValue().getLastEntryId();
        } else {
            this.lastAddConfirmed = -1L;
            this.lastAddPushed = -1L;
        }
        this.delayedWriteFailedBookies = new HashMap<>();
        this.closeFutures = new ArrayList<>();
    }

    public void cancel() {
        isCancelled.set(true);
    }

    public void printState() {
        logger.logInfo("------------ Ledger Write Handle State -------------" + System.lineSeparator()
            + "Ledger metadata version: " + versionedMetadata.getVersion() + System.lineSeparator()
            + "Ledger metadata: " + versionedMetadata.getValue() + System.lineSeparator()
            + "pendingAddsSequenceHead: "+ pendingAddsSequenceHead + System.lineSeparator()
            + "lastAddPushed: "+ lastAddPushed + System.lineSeparator()
            + "lastAddConfirmed: "+ lastAddConfirmed + System.lineSeparator()
            + "changingEnsemble: "+ changingEnsemble
            + "----------------------------------------------");
    }

    public Versioned<LedgerMetadata> getCachedLedgerMetadata() {
        return versionedMetadata;
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
        return addEntry(value, false);
    }

    public CompletableFuture<Entry> recoveryAddEntry(String value) {
        return addEntry(value, true);
    }

    private CompletableFuture<Entry> addEntry(String value, boolean isRecoveryAdd) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        lastAddPushed++;
        Entry entry = new Entry(lm().getLedgerId(), lastAddPushed, value);
        PendingAddOp addOp = new PendingAddOp(
                messageSender,
                entry,
                lm().getCurrentEnsemble(),
                lm().getWriteQuorum(),
                lm().getAckQuorum(),
                this,
                isRecoveryAdd,
                future,
                isCancelled);

        addOp.begin();
        pendingAddOps.add(addOp);

        return future;
    }

    void handleUnrecoverableErrorDuringAdd(String rc, boolean isRecoveryOp) {
        if (isRecoveryOp) {
            errorOutPendingAdds(rc);
        } else {
            closeInternal(rc);
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
        logger.logDebug("Closing ledger: " + getLedgerId());
        if (lm().getStatus() == LedgerStatus.CLOSED) {
            logger.logDebug("Ledger " + getLedgerId() + " already closed");
            closeFutures.stream().forEach(f -> f.complete(versionedMetadata));
            return CompletableFuture.completedFuture(versionedMetadata);
        }

        pendingClose = true;
        lm().setStatus(LedgerStatus.CLOSED);
        lm().setLastEntryId(lastAddConfirmed);
        errorOutPendingAdds(rc);

        logger.logDebug("Sending metadata update with closed status for ledger: " + getLedgerId());
        return updateMetadataStore(versionedMetadata)
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                    if (t != null) {
                        logger.logError("Ledger Close failed", t);
                    } else {
                        versionedMetadata = vlm;
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

    private CompletableFuture<Versioned<LedgerMetadata>> updateMetadataStore(Versioned<LedgerMetadata> vlm) {
        return ledgerManager.updateLedgerMetadata(vlm)
                .thenApply(this::checkForCancellation)
                .thenApply((Versioned<LedgerMetadata> vlmUpdated) -> {
                    logger.logDebug("Metadata updated in metadata store. New: "
                            + vlmUpdated.getValue()
                            + " From: " + versionedMetadata.getValue());

                    return vlmUpdated;
                });
    }

    private void changeEnsemble(Map<Integer, String> failedBookies) {
        boolean closeLedgerOnFail = lm().getStatus() == LedgerStatus.OPEN;

        chooseNewEnsemble(failedBookies, versionedMetadata)
                .whenComplete((Void v, Throwable t) -> {
                    changingEnsemble = false;

                    if (t != null) {
                        ensembleChangeFailed(t, closeLedgerOnFail);
                    }
                });
    }

    private CompletableFuture<Void> chooseNewEnsemble(Map<Integer, String> failedBookies,
                                                      Versioned<LedgerMetadata> vlm) {
        Map<Integer, String> bookiesToReplace = validFailedBookies(failedBookies);
        if (bookiesToReplace.isEmpty()) {
            logger.logDebug("Ensemble changed cancelled - Ignoring bookie write failure for bookie that is no longer a member of the current ensemble");
            throw new OperationCancelledException();
        } else if (lm().getStatus() == LedgerStatus.CLOSED) {
            logger.logDebug("Ensemble changed cancelled - ledger already closed");
            throw new BkException("Ledger already closed", ReturnCodes.Ledger.LEDGER_CLOSED);
        }

        // work on a copy and replace it at the end
        Versioned<LedgerMetadata> copyOfMetadata = new Versioned<>(
                new LedgerMetadata(vlm.getValue()), vlm.getVersion());

        logger.logDebug("Starting ensemble change due to failure in bookies: " + bookiesToReplace.values());
        changingEnsemble = true;

        return ledgerManager.getAvailableBookies()
                .thenApply(this::checkForCancellation)
                .thenCompose((List<String> availableBookies) -> {
                    logger.logDebug("Available bookies: " + availableBookies);
                    availableBookies.removeAll(copyOfMetadata.getValue().getCurrentEnsemble());
                    logger.logDebug("Available bookies not in current ensemble: " + availableBookies);
                    if (availableBookies.size() < bookiesToReplace.size()) {
                        logger.logError("Couldn't add a new ensemble, not enough bookies");
                        throw new BkException("Not enough bookies for ensemble change",
                                    ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES);
                    } else {
                        logger.logDebug("Enough available bookies for ensemble change");
                        Collections.shuffle(availableBookies);
                        Set<Integer> replacedBookieIndices = new HashSet<>();

                        List<String> newEnsemble = new ArrayList<>(copyOfMetadata.getValue().getCurrentEnsemble());
                        int replaceIndex = 0;
                        for (int bookieIndex : bookiesToReplace.keySet()) {
                            String newBookie = availableBookies.get(replaceIndex);
                            newEnsemble.set(bookieIndex, newBookie);
                            replacedBookieIndices.add(bookieIndex);
                            replaceIndex++;
                        }

                        if (lastAddConfirmed + 1 == copyOfMetadata.getValue().getEnsembles().lastKey()) {
                            logger.logDebug("Replacing last ensemble: "
                                    + copyOfMetadata.getValue().getCurrentEnsemble()
                                    + " with new ensemble: " + newEnsemble);
                            copyOfMetadata.getValue().replaceCurrentEnsemble(newEnsemble);
                        } else {
                            logger.logDebug("Appending new ensemble: " + newEnsemble);
                            copyOfMetadata.getValue().addEnsemble(lastAddConfirmed + 1, newEnsemble);
                        }

                        /*
                            If we're in recovery then we do not update the ensemble now as else it
                            can affect the recovery reads and cause the ledger to be truncated.
                            If the ledger is open, then we commit the ensemble change and then update any
                            pending add ops of the ensemble change.
                         */
                        if (copyOfMetadata.getValue().getStatus() == LedgerStatus.IN_RECOVERY) {
                            logger.logDebug("Ensemble changed locally only for recovery writer. Current ensemble: "
                                    + copyOfMetadata.getValue().getCurrentEnsemble()
                                    + " From: " + versionedMetadata.getValue().getCurrentEnsemble());
                            versionedMetadata = copyOfMetadata;
                            unsetSuccessAndSendWriteRequest(newEnsemble, replacedBookieIndices);
                            return CompletableFuture.completedFuture(null);
                        } else {
                            return updateMetadataStore(copyOfMetadata)
                                    .thenAccept((Versioned<LedgerMetadata> updatedVlm) -> {
                                        versionedMetadata = updatedVlm;
                                        unsetSuccessAndSendWriteRequest(newEnsemble, replacedBookieIndices);
                                        logger.logDebug("Ensemble change updated in store and pending adds updated");
                                    });
                        }
                    }
                })
                .thenCompose((Void v) -> {
                    if (!delayedWriteFailedBookies.isEmpty() && delayedWriteFailedBookies.values().stream()
                            .anyMatch(x -> versionedMetadata.getValue().getCurrentEnsemble().contains(x))) {
                        logger.logInfo("More failed bookies during last ensemble change. Triggered new ensemble change.");
                        return chooseNewEnsemble(delayedWriteFailedBookies, versionedMetadata);
                    } else {
                        delayedWriteFailedBookies.clear();
                        logger.logDebug("Ensemble change completed successfully");
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    private Map<Integer, String> validFailedBookies(Map<Integer, String> failedBookies) {
        Map<Integer, String> candidates = new HashMap<>(delayedWriteFailedBookies);
        candidates.putAll(failedBookies);

        Map<Integer, String> toReplace = new HashMap<>();
        for (Map.Entry<Integer, String> candidate : candidates.entrySet()) {
            if (lm().getCurrentEnsemble().contains(candidate.getValue())) {
                toReplace.put(candidate.getKey(), candidate.getValue());
            }
        }

        delayedWriteFailedBookies.clear();

        return toReplace;
    }

    private void ensembleChangeFailed(Throwable t, boolean closeLedger) {
        t = Futures.unwrap(t);
        if (t instanceof MetadataException || t instanceof BkException) {
            String code = t instanceof MetadataException
                    ? ((MetadataException)t).getCode()
                    : ((BkException)t).getCode();
            if (closeLedger) {
                logger.logError("The ensemble change has failed due to code: " + code
                        + " Will try to close the ledger", t);
                closeInternal(code);
            } else {
                logger.logError("The ensemble change has failed due to code: " + code
                        + " Erroring out pending add ops", t);
                errorOutPendingAdds(code);
            }
        } else if (Futures.unwrap(t) instanceof OperationCancelledException) {
            logger.logInfo("The ensemble change has been cancelled");
        } else {
            if (closeLedger) {
                logger.logError("The ensemble change has failed due to an unexpected error. " +
                        " Will try to close the ledger.", t);
                closeInternal(ReturnCodes.UNEXPECTED_ERROR);
            } else {
                logger.logError("The ensemble change has failed due to an unexpected error." +
                        " Erroring out pending add ops", t);
                errorOutPendingAdds(ReturnCodes.UNEXPECTED_ERROR);
            }
        }
    }

    void unsetSuccessAndSendWriteRequest(List<String> ensemble, final Set<Integer> replacedBookieIndices) {
        for (PendingAddOp pendingAddOp : pendingAddOps) {
            for (Integer bookieIndex : replacedBookieIndices) {
                pendingAddOp.unsetSuccessAndSendWriteRequest(ensemble, bookieIndex);
            }
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

//    private void changeEnsembleOld(Map<Integer, String> failedBookies) {
//        Map<Integer, String> bookiesToReplace = validFailedBookies(failedBookies);
//        if (bookiesToReplace.isEmpty()) {
//            logger.logDebug("LedgerWriteHandle : Ignoring bookie write failure for bookie that is no longer a member of the current ensemble");
//            return;
//        } else if (lm().getStatus() == LedgerStatus.CLOSED) {
//            logger.logDebug("LedgerWriteHandle : Ensemble changed cancelled - ledger already closed");
//            return;
//        }
//
//        logger.logDebug("LedgerWriteHandle : Changing the ensemble due to failure in bookies: " + bookiesToReplace.values());
//        changingEnsemble = true;
//
//        // work on a copy and replace it at the end
//        Versioned<LedgerMetadata> copyOfMetadata = new Versioned<>(
//                new LedgerMetadata(lm()), versionedMetadata.getVersion());
//
//        ledgerManager.getAvailableBookies()
//                .thenApply(this::checkForCancellation)
//                .thenCompose((List<String> availableBookies) -> {
//                    logger.logDebug("LedgerWriteHandle : Available bookies: " + availableBookies);
//                    availableBookies.removeAll(copyOfMetadata.getValue().getCurrentEnsemble());
//                    logger.logDebug("LedgerWriteHandle : Available bookies not in current ensemble: " + availableBookies);
//                    if (availableBookies.size() < bookiesToReplace.size()) {
//                        logger.logError("LedgerWriteHandle : Couldn't add a new ensemble, not enough bookies");
//                        if (lm().getStatus() == LedgerStatus.IN_RECOVERY) {
//                            // don't close if we're in recovery, else we'd truncate data
//                            return Futures.failedFuture(new BkException("Not enough bookies to change the ensemble",
//                                    ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES));
//                        } else {
//                            return closeInternal(ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES);
//                        }
//                    } else {
//                        logger.logDebug("LedgerWriteHandle : Enough available bookies");
//                        Collections.shuffle(availableBookies);
//                        Set<Integer> replacedBookieIndices = new HashSet<>();
//
//                        List<String> newEnsemble = new ArrayList<>(copyOfMetadata.getValue().getCurrentEnsemble());
//                        int replaceIndex = 0;
//                        for (int bookieIndex : bookiesToReplace.keySet()) {
//                            String newBookie = availableBookies.get(replaceIndex);
//                            newEnsemble.set(bookieIndex, newBookie);
//                            replacedBookieIndices.add(bookieIndex);
//                            replaceIndex++;
//                        }
//
//                        if (lastAddConfirmed + 1 == copyOfMetadata.getValue().getEnsembles().lastKey()) {
//                            logger.logDebug("LedgerWriteHandle : Replacing last ensemble: "
//                                    + copyOfMetadata.getValue().getCurrentEnsemble()
//                                    + " with new ensemble: " + newEnsemble);
//                            copyOfMetadata.getValue().replaceCurrentEnsemble(newEnsemble);
//                        } else {
//                            logger.logDebug("LedgerWriteHandle : Appending new ensemble: " + newEnsemble);
//                            copyOfMetadata.getValue().addEnsemble(lastAddConfirmed + 1, newEnsemble);
//                        }
//
//                        return ledgerManager.updateLedgerMetadata(copyOfMetadata)
//                                .thenApply(this::checkForCancellation)
//                                .thenApply((Versioned<LedgerMetadata> vlm) -> {
//                                    logger.logDebug("LedgerWriteHandle : Metadata updated. Current ensemble: "
//                                            + vlm.getValue().getCurrentEnsemble()
//                                            + " From: " + versionedMetadata.getValue().getCurrentEnsemble());
//                                    versionedMetadata = vlm;
//                                    unsetSuccessAndSendWriteRequest(newEnsemble, replacedBookieIndices);
//
//                                    return versionedMetadata;
//                                });
//                    }
//                })
//                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
//                    changingEnsemble = false;
//
//                    if (t != null) {
//                        ensembleChangeFailed(t);
//                    } else if (!delayedWriteFailedBookies.isEmpty() && delayedWriteFailedBookies.values().stream()
//                            .anyMatch(x -> vlm.getValue().getCurrentEnsemble().contains(x))) {
//                        logger.logInfo("LedgerWriteHandle : More failed bookies during last ensemble change. Triggered new ensemble change.");
//                        changeEnsemble(delayedWriteFailedBookies);
//                    } else {
//                        logger.logDebug("LedgerWriteHandle : Ensemble change complete");
//                    }
//                });
//    }
}
