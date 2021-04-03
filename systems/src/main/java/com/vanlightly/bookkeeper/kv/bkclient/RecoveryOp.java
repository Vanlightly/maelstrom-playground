package com.vanlightly.bookkeeper.kv.bkclient;

import com.vanlightly.bookkeeper.FutureRetries;
import com.vanlightly.bookkeeper.OperationCancelledException;
import com.vanlightly.bookkeeper.ReturnCodes;
import com.vanlightly.bookkeeper.kv.log.Position;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.Versioned;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecoveryOp {
    LedgerHandle lh;
    int readCount;
    int writeCount;
    boolean readsComplete;
    CompletableFuture<Void> callerFuture;
    AtomicBoolean isCancelled;

    public RecoveryOp(LedgerHandle lh,
                      CompletableFuture<Void> callerFuture,
                      AtomicBoolean isCancelled) {
        this.lh = lh;
        this.callerFuture = callerFuture;
        this.isCancelled = isCancelled;
        this.readCount = 0;
        this.writeCount = 0;
        this.readsComplete = false;
    }

    public void begin() {
        lh.readExplicitLac()
                .thenCompose((Long lacRead) -> {
                    long entryIdOfCurrentEnsemble = lh.versionedMetadata.getValue()
                            .getEnsembles()
                            .lastKey();
                    long lac = Math.max(lacRead, entryIdOfCurrentEnsemble);
                    lh.setLastAddConfirmed(lac);
                    lh.setLastAddPushed(lac);
                    return readNext(lh, new Position(lh.getLedgerId(), lac+1));
                })
                .whenComplete((Position pos, Throwable t1) -> {
                    // have now read up to as far as we can go, or an error has occurred
                    if (isCancelled.get()) {
                        callerFuture.completeExceptionally(new OperationCancelledException());
                    } else {
                        if (t1 != null) {
                            if (t1 instanceof BkException) {
                                String code = ((BkException) t1).getCode();
                                if (code.equals(ReturnCodes.Ledger.LESS_THAN_ACK_QUORUM)) {
                                    // we reached the end of the ledger, there still may be
                                    // some write results pending, but if not then close the ledger now
                                    readsComplete = true;
                                    if (readCount == writeCount) {
                                        closeLedger();
                                    }
                                }
                            }
                        }
                    }
                });
    }

    private CompletableFuture<Position> readNext(LedgerHandle lh, Position prev) {
        CompletableFuture<Position> future = new CompletableFuture<>();

        lh.quorumRead(prev.getEntryId() + 1)
                .thenCompose((List<Entry> entries) -> {
                    if (isCancelled.get()) {
                        callerFuture.completeExceptionally(new OperationCancelledException());
                        throw new OperationCancelledException();
                    } else {
                        readCount++;

                        // write back the entry to the ledger to ensure it reaches write quorum
                        Entry e = entries.get(0);
                        lh.addEntry(e.getValue())
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

                                            if (readsComplete && readCount == writeCount) {
                                                closeLedger();
                                            }
                                        }
                                    }
                                });

                        // read the next entry
                        Position currPos = new Position(e.getLedgerId(),
                                e.getEntryId());
                        return readNext(lh, currPos);
                    }
                });

        return future;
    }

    private void closeLedger() {
        lh.close()
            .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t2) -> {
                if (isCancelled.get()) {
                    callerFuture.completeExceptionally(new OperationCancelledException());
                } else if (t2 != null) {
                    callerFuture.completeExceptionally(t2);
                } else {
                    callerFuture.complete(null);
                }
            });
    }
}
