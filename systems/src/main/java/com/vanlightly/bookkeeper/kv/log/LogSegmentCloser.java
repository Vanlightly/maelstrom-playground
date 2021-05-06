package com.vanlightly.bookkeeper.kv.log;

import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;
import com.vanlightly.bookkeeper.ManagerBuilder;
import com.vanlightly.bookkeeper.MessageSender;
import com.vanlightly.bookkeeper.kv.bkclient.*;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.LedgerStatus;
import com.vanlightly.bookkeeper.metadata.Versioned;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LogSegmentCloser extends LogClient {
    private Logger logger = LogManager.getLogger(this.getClass().getSimpleName());
    private LedgerReadHandle readHandle;
    private LedgerWriteHandle writeHandle;
    private RecoveryOp recoveryOp;

    public LogSegmentCloser(ManagerBuilder builder,
                            MessageSender messageSender) {
        super(builder, messageSender, null);
    }

    @Override
    public void cancel() {
        isCancelled.set(true);
        if (readHandle != null) {
            readHandle.cancel();
        }

        if (writeHandle != null) {
            writeHandle.cancel();
        }
    }

    public CompletableFuture<Position> closeLastSegment() {
        CompletableFuture<Position> future = new CompletableFuture<>();

        // if there are no segments, then complete the future successfully
        // else get the last ledgers metadata and start a recovery op
        metadataManager.getLedgerList()
            .thenAccept((Versioned<List<Long>> vll) -> {
                if (vll.getValue().isEmpty()) {
                    future.complete(new Position(-1, -1));
                } else {
                    long ledgerId = vll.getValue().get(vll.getValue().size()-1);

                    ledgerManager.getLedgerMetadata(ledgerId)
                            .thenApply(this::checkForCancellation)
                            .thenAccept((Versioned<LedgerMetadata> vlm) -> {
                                if (vlm.getValue().getStatus() == LedgerStatus.CLOSED) {
                                    // already closed, nothing more to do
                                    logger.logDebug("Ledger already closed. Ledger: " + ledgerId);
                                    future.complete(new Position(vlm.getValue().getLedgerId(),
                                            vlm.getValue().getLastEntryId()));
                                } else {
                                    vlm.getValue().setStatus(LedgerStatus.IN_RECOVERY);
                                    ledgerManager.updateLedgerMetadata(vlm)
                                            .thenAccept((Versioned<LedgerMetadata> vlm2) -> {
                                                readHandle = new LedgerReadHandle(ledgerManager, messageSender, vlm2);
                                                writeHandle = new LedgerWriteHandle(ledgerManager, messageSender, vlm2);
                                                recoveryOp = new RecoveryOp(readHandle, writeHandle, future, isCancelled);
                                                recoveryOp.begin();
                                            })
                                            .whenComplete((Void v, Throwable t) -> handleError(t, future));
                                }
                            })
                            .whenComplete((Void v, Throwable t) -> handleError(t, future));
                }
            })
            .whenComplete((Void v, Throwable t) -> handleError(t, future));

        return future;
    }

    private void handleError(Throwable t, CompletableFuture<Position> future) {
        if (isError(t)) {
            logger.logError("Failed closing a log segment", t);
            if (!future.isDone()) {
                future.completeExceptionally(t);
            }
        }
    }
}
