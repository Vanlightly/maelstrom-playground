package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.BkException;
import com.vanlightly.bookkeeper.kv.bkclient.Entry;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerHandle;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.LedgerStatus;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.Futures;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/*
    Writes to the log and updates the cursor and committed index.
    Abstracts away ledgers and handles - is at the log abstraction level.
 */
public class LogWriter extends LogClient {

    private Versioned<List<Long>> cachedLedgerList;

    public LogWriter(ManagerBuilder managerBuilder,
                     ObjectMapper mapper,
                     Logger logger,
                     MessageSender messageSender,
                     BiConsumer<Position, Op> cursorUpdater) {
        super(managerBuilder, mapper, logger,
                messageSender, cursorUpdater);
        this.cachedLedgerList = new Versioned<>(new ArrayList<>(), -1);
    }

    public CompletableFuture<Void> start() {
        return metadataManager.getLedgerList()
                .thenApply(this::checkForCancellation)
                .thenCompose((Versioned<List<Long>> vll) -> {
                    cachedLedgerList = vll;
                    return createWritableLedgerHandle();
                })
                .thenApply(this::checkForCancellation)
                .thenAccept((Void v) -> {
                    Position p = new Position(lh.getLedgerId(), -1L);
                    cursorUpdater.accept(p, null);
                });
    }

    public boolean isHealthy() {
        return lh.getCachedLedgerMetadata().getValue().getStatus().equals(LedgerStatus.OPEN);
    }

    public void printState() {
        logger.logInfo("-------------- Log Writer state -------------");
        if (lh == null) {
            logger.logInfo("No ledger handle");
        } else {
            lh.printState();
        }
        logger.logInfo("---------------------------------------------");
    }

    public CompletableFuture<Void> close() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        lh.close()
                .thenApply(this::checkForCancellation)
                .thenAccept((Versioned<LedgerMetadata> vlm) -> {
                    logger.logInfo("Ledger closed successfully");
                    future.complete(null);
                })
                .whenComplete((Void v, Throwable t) -> {
                    if (isError(t)) {
                        logger.logError("Failed to close ledger properly");
                        future.completeExceptionally(t);
                    } else {
                        future.complete(null);
                    }
                });

        return future;
    }

    public CompletableFuture<Void> write(String value) {
        return lh.addEntry(value)
                .thenApply(this::checkForCancellation)
                .thenAccept((Entry entry) -> {
                    Op op = Op.stringToOp(entry.getValue());
                    Position pos = new Position(entry.getLedgerId(), entry.getEntryId());
                    cursorUpdater.accept(pos, op);
                });
    }

    private CompletableFuture<Void> createWritableLedgerHandle() {
        CompletableFuture<Void> future = new CompletableFuture<>();

        ledgerManager.getAvailableBookies()
                .thenApply(this::checkForCancellation)
                .thenCompose((List<String> availableBookies) -> createLedgerMetadata(availableBookies))
                .thenApply(this::checkForCancellation)
                .thenCompose((Versioned<LedgerMetadata> vlm) -> appendToLedgerList(vlm))
                .thenApply(this::checkForCancellation)
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                    if (t == null) {
                        lh = new LedgerHandle(mapper, ledgerManager, messageSender,
                                logger, isCancelled, vlm);
                        future.complete(null);
                    } else if (isError(t)) {
                        future.completeExceptionally(t);
                    } else {
                        future.complete(null);
                    }
                });
        return future;
    }

    private CompletableFuture<Versioned<LedgerMetadata>> createLedgerMetadata(List<String> availableBookies) {
        int writeQuorum = 3;
        if (availableBookies.size() < writeQuorum) {
            return Futures.failedFuture(
                    new BkException("Not enough non-faulty bookies",
                            ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES));
        } else {
            return ledgerManager.getLedgerId()
                    .thenApply(this::checkForCancellation)
                    .thenCompose((Long ledgerId) -> {
                        List<String> ensemble = randomSubset(availableBookies, writeQuorum);
                        LedgerMetadata lmd = new LedgerMetadata(ledgerId, writeQuorum, 2, ensemble);
                        return ledgerManager.createLedgerMetadata(lmd);
                    });
        }
    }

    private CompletableFuture<Versioned<LedgerMetadata>> appendToLedgerList(Versioned<LedgerMetadata> ledgerMetadata) {
        CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();

        cachedLedgerList.getValue().add(ledgerMetadata.getValue().getLedgerId());
        metadataManager.updateLedgerList(cachedLedgerList)
                .thenAccept((Versioned<List<Long>> vll) -> {
                    cachedLedgerList = vll;
                    future.complete(ledgerMetadata);
                })
                .whenComplete((Void v, Throwable t) -> {
                    if (t != null) {
                        future.completeExceptionally(t);
                    }
                });

        return future;
    }

    private List<String> randomSubset(List<String> bookies, int subsetSize) {
        List<String> copy = new ArrayList<>(bookies);
        Collections.shuffle(copy);
        return copy.stream().limit(subsetSize)
                .sorted()
                .collect(Collectors.toList());
    }
}
