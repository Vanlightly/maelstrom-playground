package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.BkException;
import com.vanlightly.bookkeeper.kv.bkclient.Entry;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerHandle;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.Versioned;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class LogWriter extends LogClient {

    public LogWriter(MetadataManager metadataManager,
                     LedgerManager ledgerManager,
                     ObjectMapper mapper,
                     Logger logger,
                     MessageSender messageSender,
                     BiConsumer<Position, Op> cursorUpdater) {
        super(metadataManager, ledgerManager, mapper, logger,
                messageSender, cursorUpdater);
    }

    public CompletableFuture<Void> start() {
        return createWritableLedgerHandle();
    }

    public CompletableFuture<Void> close() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        lh.close().whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                if (t != null) {
                    logger.logError("Failed to close ledger properly");
                    future.completeExceptionally(t);
                } else {
                    logger.logInfo("Ledger closed successfully");
                    future.complete(null);
                }
            });

        return future;
    }

    public CompletableFuture<Void> write(String value) {
        return lh.addEntry(value)
                .thenAccept((Entry entry) -> {
                    Op op = Op.stringToOp(entry.getValue());
                    Position pos = new Position(entry.getLedgerId(), entry.getEntryId());
                    cursorUpdater.accept(pos, op);
                });
    }

    private CompletableFuture<Void> createWritableLedgerHandle() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        ledgerManager.getAvailableBookies()
                .thenCompose((List<String> availableBookies) -> {
                    checkForCancellation();
                    return createLedgerMetadata(availableBookies);
                })
                .thenCompose((Versioned<LedgerMetadata> vlm) -> {
                    checkForCancellation();
                    return appendToLedgerList(vlm);
                })
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable ct) -> {
                    checkForCancellation();
                    if (ct != null) {
                        future.completeExceptionally(ct);
                    } else {
                        lh = new LedgerHandle(mapper, ledgerManager, messageSender,
                                logger, isCancelled, vlm);
                        future.complete(null);
                    }
                });
        return future;
    }

    private CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(List<String> availableBookies) {
        int writeQuorum = 3;
        if (availableBookies.size() < writeQuorum) {
            return FutureRetries.nonRetryableFailedFuture(
                    new BkException("Not enough non-faulty bookies",
                            ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES));
        } else {
            LedgerMetadata lmd = new LedgerMetadata(-1L, writeQuorum, 2, availableBookies);
            return ledgerManager.createLedgerMetadata(lmd);
        }
    }

    private CompletableFuture<Versioned<LedgerMetadata>> appendToLedgerList(Versioned<LedgerMetadata> ledgerMetadata) {
        CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();

        metadataManager.getLedgerList()
                .thenCompose((Versioned<List<Long>> vll) -> {
                    checkForCancellation();
                    vll.getValue().add(ledgerMetadata.getValue().getLedgerId());
                    return metadataManager.updateLedgerList(vll);
                })
                .whenComplete((Versioned<List<Long>> newLedgerList, Throwable t) -> {
                    checkForCancellation();
                    if (t != null) {
                        future.completeExceptionally(t);
                    } else {
                        future.complete(ledgerMetadata);
                    }
                });

        return future;
    }

    private void checkForCancellation() {
        if (isCancelled.get()) {
            throw new OperationCancelledException();
        }
    }
//
//    private static class InitResult {
//        int leaderEpoch;
//        LedgerHandle lh;
//
//        public InitResult(int leaderEpoch, LedgerHandle lh) {
//            this.leaderEpoch = leaderEpoch;
//            this.lh = lh;
//        }
//
//        public int getLeaderEpoch() {
//            return leaderEpoch;
//        }
//
//        public LedgerHandle getLh() {
//            return lh;
//        }
//    }
}
