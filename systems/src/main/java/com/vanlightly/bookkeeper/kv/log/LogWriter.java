package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.BkException;
import com.vanlightly.bookkeeper.kv.bkclient.Entry;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerWriteHandle;
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

    A single log writer instance only ever writes to a single ledger. When
    an event occurs such as the ledger having been closed, fenced or there
    not being enough bookies to write, then the writer aborts.
 */
public class LogWriter extends LogClient {

    private LedgerWriteHandle writeHandle;
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

    public CompletableFuture<Void> start(Versioned<List<Long>> cachedLedgerList) {
        this.cachedLedgerList = cachedLedgerList;

        return createWritableLedgerHandle()
                .thenApply(this::checkForCancellation)
                .thenAccept((Void v) -> {
                    Position p = new Position(writeHandle.getLedgerId(), -1L);
                    cursorUpdater.accept(p, null);
                });
    }

    @Override
    public void cancel() {
        isCancelled.set(true);
        if (writeHandle != null) {
            writeHandle.cancel();
        }
    }

    public boolean isHealthy() {
        return writeHandle.getCachedLedgerMetadata().getValue().getStatus().equals(LedgerStatus.OPEN);
    }

    public Versioned<List<Long>> getCachedLedgerList() {
        return cachedLedgerList;
    }

    public void printState() {
        logger.logInfo("-------------- Log Writer state -------------");
        if (writeHandle == null) {
            logger.logInfo("No ledger handle");
        } else {
            writeHandle.printState();
        }
        logger.logInfo("---------------------------------------------");
    }

    public CompletableFuture<Void> close() {
        CompletableFuture<Void> future = new CompletableFuture<>();
        writeHandle.close()
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                    if (isError(t)) {
                        future.completeExceptionally(t);
                    } else {
                        future.complete(null);
                    }
                });

        return future;
    }

    public CompletableFuture<Void> write(String value) {
        return writeHandle.addEntry(value)
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
                        writeHandle = new LedgerWriteHandle(mapper, ledgerManager, messageSender, logger, vlm);
                        logger.logDebug("Created new ledger handle for writer");
                        writeHandle.printState();
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
        if (availableBookies.size() < Constants.Bookie.WriteQuorum) {
            return Futures.failedFuture(
                    new BkException("Not enough non-faulty bookies",
                            ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES));
        } else {
            return ledgerManager.getLedgerId()
                    .thenApply(this::checkForCancellation)
                    .thenCompose((Long ledgerId) -> {
                        List<String> ensemble = randomSubset(availableBookies, Constants.Bookie.WriteQuorum);
                        LedgerMetadata lmd = new LedgerMetadata(ledgerId, Constants.Bookie.WriteQuorum,
                                Constants.Bookie.AckQuorum, ensemble);
                        logger.logDebug("Sending create ledger metadata request: " + lmd);
                        return ledgerManager.createLedgerMetadata(lmd);
                    });
        }
    }

    private CompletableFuture<Versioned<LedgerMetadata>> appendToLedgerList(Versioned<LedgerMetadata> ledgerMetadata) {
        CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();

        cachedLedgerList.getValue().add(ledgerMetadata.getValue().getLedgerId());
        metadataManager.updateLedgerList(cachedLedgerList)
                .thenAccept((Versioned<List<Long>> vll) -> {
                    logger.logDebug("Appended ledger to list: " + vll.getValue());
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
