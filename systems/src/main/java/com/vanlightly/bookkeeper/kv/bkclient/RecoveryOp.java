package com.vanlightly.bookkeeper.kv.bkclient;

import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.log.Position;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.Futures;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoveryOp {
    Logger logger;
    LedgerHandle lh;
    int readCount;
    int writeCount;
    boolean readsComplete;
    CompletableFuture<Void> callerFuture;
    AtomicBoolean isCancelled;

    public RecoveryOp(Logger logger,
                      LedgerHandle lh,
                      CompletableFuture<Void> callerFuture,
                      AtomicBoolean isCancelled) {
        this.logger = logger;
        this.lh = lh;
        this.callerFuture = callerFuture;
        this.isCancelled = isCancelled;
        this.readCount = 0;
        this.writeCount = 0;
        this.readsComplete = false;
    }

    public void begin() {
        logger.logDebug("RECOVERY: Starting recovery op");
        lh.readLac()
                .thenCompose((Result<Entry> lacResult) -> {
                    long lac = -1L;
                    long entryIdOfCurrentEnsemble = lh.versionedMetadata.getValue()
                            .getEnsembles()
                            .lastKey();

                    switch (lacResult.getCode()) {
                        case ReturnCodes.OK:
                            lac = Math.max(lacResult.getData().getLac(), entryIdOfCurrentEnsemble-1L);
                            break;
                        case ReturnCodes.Ledger.UNKNOWN:
                            lac = entryIdOfCurrentEnsemble - 1L;
                            break;
                        default:
                            return Futures.failedFuture(
                                    new BkException("Could not read LAC during rcovery", lacResult.getCode()));
                    };
                    lh.setLastAddConfirmed(lac);
                    lh.setLastAddPushed(lac);
                    logger.logDebug("RECOVERY: Starting recovery with LAC " + lac);
                    return readNext(lh, new Position(lh.getLedgerId(), lac));
                })
                .whenComplete((Position pos, Throwable t1) -> {
                    // have now read up to as far as we can go, or an error has occurred
                    if (isCancelled.get()) {
                        callerFuture.completeExceptionally(new OperationCancelledException());
                    } else if (t1 != null) {
                        callerFuture.completeExceptionally(t1);
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

    private CompletableFuture<Position> readNext(LedgerHandle lh, Position prev) {
        logger.logDebug("RECOVERY: Last read: " + prev);
        return lh.parallelRead(prev.getEntryId() + 1, Commands.Bookie.READ_ENTRY, true)
                .thenCompose((Result<Entry> result) -> {
                    if (isCancelled.get()) {
                        callerFuture.completeExceptionally(new OperationCancelledException());
                        throw new OperationCancelledException();
                    } else {
                        if (result.getCode().equals(ReturnCodes.OK)) {
                            readCount++;

                            logger.logDebug("RECOVERY: Read " + readCount + " successful " + result.getData() +
                                    ". Writing entry back to ensemble.");

                            // write back the entry to the ledger to ensure it reaches write quorum
                            lh.addEntry(result.getData().getValue())
                                    .whenComplete((Entry e2, Throwable t2) -> {
                                        if (!isCancelled.get()) {
                                            if (t2 != null) {
                                                // the write failed, so we need to abort the recovery op
                                                isCancelled.set(true);
                                                callerFuture.completeExceptionally(t2);
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
                            return readNext(lh, currPos);
                        } else if (result.getCode().equals(ReturnCodes.Ledger.NO_QUORUM)) {
                            // the previous read was the last good entry
                            return CompletableFuture.completedFuture(prev);
                        } else {
                            // we don't know if the current entry exists or not
                            throw new BkException("Too many unknown responses", ReturnCodes.Ledger.UNKNOWN);
                        }
                    }
                });
    }

    private void closeLedger() {
        logger.logDebug("RECOVERY: Closing the ledger");
        lh.close()
            .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t2) -> {
                if (isCancelled.get()) {
                    callerFuture.completeExceptionally(new OperationCancelledException());
                } else if (t2 != null) {
                    logger.logError("RECOVERY: Failed to close the ledger", t2);
                    callerFuture.completeExceptionally(t2);
                } else {
                    logger.logDebug("RECOVERY: Ledger closed");
                    callerFuture.complete(null);
                }
            });
    }
}
