package com.vanlightly.bookkeeper.kv.bkclient;

import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.log.Position;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.Futures;
import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoveryOp {
    private Logger logger = LogManager.getLogger(this.getClass().getName());
    LedgerReadHandle readHandle;
    LedgerWriteHandle writeHandle;
    int readCount;
    int writeCount;
    boolean readsComplete;
    CompletableFuture<Position> callerFuture;
    AtomicBoolean isCancelled;

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
    }

    public void begin() {
        logger.logDebug("RECOVERY: Starting recovery op");
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
                        logger.logDebug("RECOVERY: Starting recovery with LAC " + lac);
                        return readNext(new Position(readHandle.getLedgerId(), lac));
                    } else {
                        return Futures.failedFuture(new BkException("RECOVERY: Couldn't fence enough bookies to progress", lacResult.getCode()));
                    }
                })
                .whenComplete((Position pos, Throwable t1) -> {
                    // have now read up to as far as we can go, or an error has occurred
                    if (isCancelled.get()) {
                        completeExceptionally(new OperationCancelledException());
                    } else if (t1 != null) {
                        completeExceptionally(t1);
                    } else {
                        // we reached the end of the ledger, there still may be
                        // some write results pending, but if not then close the ledger now
                        readsComplete = true;
                        if (readCount == writeCount) {
                            logger.logDebug("RECOVERY: Reads and writes complete.");
                            closeLedger();
                        } else {
                            logger.logDebug("RECOVERY: Reads complete. Waiting for writes to complete");
                        }
                    }
                });
    }

    private CompletableFuture<Position> readNext(Position prev) {
        logger.logDebug("RECOVERY: Last read: " + prev);
        return readHandle.recoveryRead(prev.getEntryId() + 1)
                .thenCompose((Result<Entry> result) -> {
                    if (isCancelled.get()) {
                        completeExceptionally(new OperationCancelledException());
                        throw new OperationCancelledException();
                    } else {
                        if (result.getCode().equals(ReturnCodes.OK)) {
                            readCount++;

                            logger.logDebug("RECOVERY: Read " + readCount + " successful " + result.getData() +
                                    ". Writing entry back to ensemble.");

                            // write back the entry to the ledger to ensure it reaches write quorum
                            writeHandle.recoveryAddEntry(result.getData().getValue())
                                    .whenComplete((Entry e2, Throwable t2) -> {
                                        if (!isCancelled.get()) {
                                            if (t2 != null) {
                                                // the write failed, so we need to abort the recovery op
                                                // and all pending actions of the ledger handle
                                                logger.logError("RECOVERY: Add failed. Cancelling recovery op.", t2);
                                                isCancelled.set(true);
                                                readHandle.cancel();
                                                writeHandle.cancel();
                                                completeExceptionally(t2);
                                                return;
                                            } else {
                                                // the write completed successfully, if this was the last
                                                // write then close the ledger
                                                writeCount++;

                                                logger.logDebug("RECOVERY: Write " + readCount + " successful " + result.getData());

                                                if (readsComplete && readCount == writeCount) {
                                                    logger.logDebug("RECOVERY: Writes complete");
                                                    closeLedger();
                                                }
                                            }
                                        }
                                    });

                            // read the next entry
                            Position currPos = new Position(result.getData().getLedgerId(),
                                    result.getData().getEntryId());
                            return readNext(currPos);
                        } else if (result.getCode().equals(ReturnCodes.Ledger.NO_QUORUM)) {
                            logger.logDebug("RECOVERY: Read " + readCount
                                    + " negative. Last committed entry is: " + prev);
                            // the previous read was the last good entry
                            return CompletableFuture.completedFuture(prev);
                        } else {
                            // we don't know if the current entry exists or not
                            logger.logDebug("RECOVERY: Read " + readCount + " unknown, cannot make progress. "
                                    + "Last successful entry is: " + prev);
                            throw new BkException("Too many unknown responses", ReturnCodes.Ledger.UNKNOWN);
                        }
                    }
                });
    }

    private void closeLedger() {
        logger.logDebug("RECOVERY: Closing the ledger");
        writeHandle.close()
            .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t2) -> {
                if (isCancelled.get()) {
                    completeExceptionally(new OperationCancelledException());
                } else if (t2 != null) {
                    logger.logError("RECOVERY: Failed to close the ledger", t2);
                    completeExceptionally(t2);
                } else {
                    logger.logDebug("RECOVERY: Ledger closed");
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
