package com.vanlightly.bookkeeper.kv.bkclient;

import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.log.Position;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.Futures;
import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoveryOp {
    private Logger logger = LogManager.getLogger(this.getClass().getSimpleName());
    private LedgerReadHandle readHandle;
    private LedgerWriteHandle writeHandle;
    private int readCount;
    private int writeCount;
    private boolean readsComplete;
    private CompletableFuture<Position> callerFuture;
    private AtomicBoolean isCancelled;
    Queue<Result<Entry>> pendingRecoveryReads;

    public RecoveryOp(LedgerReadHandle lrh,
                      LedgerWriteHandle lwh,
                      CompletableFuture<Position> callerFuture,
                      AtomicBoolean isCancelled) {
        this.readHandle = lrh;
        this.writeHandle = lwh;
        this.callerFuture = callerFuture;
        this.isCancelled = isCancelled;
        this.readCount = 0;
        this.writeCount = 0;
        this.readsComplete = false;
        this.pendingRecoveryReads = new ArrayDeque<>();
    }

    public void begin() {
        logger.logDebug("Starting recovery op");
        readHandle.readLacWithFencing()
                .thenApply(this::checkForCancellation)
                .thenCompose((Result<Entry> lacResult) -> {
                    if (lacResult.getCode() == ReturnCodes.OK) {
                        long lac = -1L;
                        long entryIdOfCurrentEnsemble = readHandle.getCachedLedgerMetadata().getValue()
                                .getEnsembles()
                                .lastKey();

                        switch (lacResult.getCode()) {
                            case ReturnCodes.OK:
                                lac = Math.max(lacResult.getData().getLac(), entryIdOfCurrentEnsemble - 1L);
                                break;
                            case ReturnCodes.Ledger.UNKNOWN:
                                lac = entryIdOfCurrentEnsemble - 1L;
                                break;
                            default:
                                return Futures.failedFuture(
                                        new BkException("Could not read LAC during rcovery", lacResult.getCode()));
                        }

                        readHandle.setLastAddConfirmed(lac);
                        writeHandle.setLastAddConfirmed(lac);
                        writeHandle.setLastAddPushed(lac);
                        logger.logDebug("Starting recovery with LAC " + lac);
                        return readUntilEnd(new Position(readHandle.getLedgerId(), lac));
                    } else {
                        return Futures.failedFuture(new BkException("Couldn't fence enough bookies to progress", lacResult.getCode()));
                    }
                })
                .whenComplete((Position pos, Throwable t) -> {
                    // have now read up to as far as we can go, or an error has occurred
                    if (isCancelled.get()) {
                        completeExceptionally(new OperationCancelledException());
                    } else if (t != null) {
                        completeExceptionally(t);
                    } else {
                        // we reached the end of the ledger, there still may be
                        // some write results pending, but if not then close the ledger now
                        readsComplete = true;
                        if (readCount == writeCount) {
                            logger.logDebug("Reads and writes complete.");
                            closeLedger();
                        } else {
                            logger.logDebug("Reads complete. Waiting for writes to complete");
                        }
                    }
                });
    }

    private CompletableFuture<Position> readUntilEnd(Position prev) {
        CompletableFuture<Position> future = new CompletableFuture<>();
        recoveryReadBatch(prev, future);
        return future;
    }

    private void recoveryReadBatch(Position prev, CompletableFuture<Position> future) {
        long fromEntryId = prev.getEntryId() + 1;
        long toEntryId = fromEntryId + 10;
        logger.logDebug("Sending recovery reads for range " + fromEntryId + "-" + toEntryId);
        AtomicBoolean readsAborted = new AtomicBoolean(false);

        // the read handle guarantees that results are ordered, so a simple loop is fine
        for (long e=fromEntryId; e<=toEntryId; e++) {

            long entryId = e;
            readHandle.recoveryRead(e)
                    .thenApply(this::checkForCancellation)
                    .thenAccept((Result<Entry> result) -> {
                        if (readsAborted.get()) {
                            // a prior read already came back negative, so skip all further read results
                            return;
                        }

                        if (result.getCode().equals(ReturnCodes.OK)) {
                            readCount++;

                            logger.logDebug("Read " + readCount + " successful " + result.getData() +
                                    ". Writing entry back to ensemble.");

                            // write back the entry to the ledger to ensure it reaches write quorum
                            writeHandle.recoveryAddEntry(result.getData().getValue())
                                    .whenComplete((Entry e2, Throwable t2) -> {
                                        if (!isCancelled.get()) {
                                            if (t2 != null) {
                                                // the write failed, so we need to abort the recovery op
                                                // and all pending actions of the ledger handle
                                                logger.logError("Add failed. Cancelling recovery op.", t2);
                                                isCancelled.set(true);
                                                readHandle.cancel();
                                                writeHandle.cancel();
                                                completeExceptionally(t2);
                                            } else {
                                                // the write completed successfully, if this was the last
                                                // write then close the ledger
                                                writeCount++;

                                                logger.logDebug("Write " + writeCount + " successful " + result.getData());

                                                if (readsComplete && readCount == writeCount) {
                                                    logger.logDebug("Writes complete");
                                                    closeLedger();
                                                }
                                            }
                                        }
                                    });

                            // continue reading if this batch has been read successfully
                            if (result.getData().getEntryId() == toEntryId) {
                                Position currPos = new Position(result.getData().getLedgerId(),
                                        result.getData().getEntryId());
                                recoveryReadBatch(currPos, future);
                            }
                        } else if (result.getCode().equals(ReturnCodes.Ledger.NO_QUORUM)) {
                            Position lastGoodPos = new Position(readHandle.getLedgerId(), entryId - 1);
                            logger.logDebug("Read negative. Last committed entry is: " + lastGoodPos);
                            // the previous read was the last good entry
                            future.complete(lastGoodPos);
                            readHandle.cancel();
                            readsAborted.set(true);
                        } else {
                            // we don't know if the current entry exists or not so we cannot make progress
                            // safely. Cancel and return "too many unknown" code.
                            logger.logDebug("Read unknown, cannot make progress. "
                                    + "Last successful entry is: " + (entryId - 1));
                            readHandle.cancel();
                            writeHandle.cancel();
                            readsAborted.set(true);
                            future.completeExceptionally(new BkException("Too many unknown responses",
                                    ReturnCodes.Ledger.UNKNOWN));
                        }
                    })
                    .whenComplete((Void v, Throwable t) -> {
                        if (isError(t)) {
                            future.completeExceptionally(t);
                        }
                    });
        }
    }

    private boolean isError(Throwable t) {
        return t != null && !(t instanceof OperationCancelledException)
                && !(Futures.unwrap(t) instanceof OperationCancelledException);
    }

    private void closeLedger() {
        logger.logDebug("Closing the ledger");
        writeHandle.close()
            .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                if (isCancelled.get()) {
                    completeExceptionally(new OperationCancelledException());
                } else if (t != null) {
                    logger.logError("Failed to close the ledger", t);
                    completeExceptionally(t);
                } else {
                    logger.logDebug("Ledger closed");
                    complete();
                }
            });
    }

    private void complete() {
        if (!callerFuture.isDone()) {
            callerFuture.complete(new Position(writeHandle.getLedgerId(), writeHandle.getLastAddConfirmed()));
        }
    }

    private void completeExceptionally(Throwable t) {
        if (!callerFuture.isDone()) {
            callerFuture.completeExceptionally(t);
        }
    }

    protected <T> T checkForCancellation(T t) {
        if (isCancelled.get()) {
            throw new OperationCancelledException();
        }

        return t;
    }
}
