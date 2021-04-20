package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.*;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.LedgerStatus;
import com.vanlightly.bookkeeper.metadata.Versioned;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/*
    Reads the log and updates the cursor (and KV store).
    Abstracts away ledgers and handles - is at the log abstraction level.
 */
public class LogReader extends LogClient {
    private LedgerHandle lh;
    private ReaderState readerState;
    private Supplier<Position> cursorView;

    private Instant lastCheckedMetadata;
    private boolean pendingMetadata;
    private Versioned<List<Long>> cachedLedgerList;

    private boolean isCatchUpReader;
    private boolean hasCaughtUp;

    public LogReader(ManagerBuilder builder,
                     ObjectMapper mapper,
                     Logger logger,
                     MessageSender messageSender,
                     BiConsumer<Position, Op> cursorUpdater,
                     Supplier<Position> cursorView,
                     boolean isCatchUpReader) {
        super(builder, mapper, logger,
                messageSender, cursorUpdater);

        this.lastCheckedMetadata = Instant.now().minus(1, ChronoUnit.DAYS);
        this.pendingMetadata = false;
        this.isCatchUpReader = isCatchUpReader;
        this.cachedLedgerList = new Versioned<>(new ArrayList<>(), -1);
        this.cursorView = cursorView;
    }

    public void start() {
        Position currPos = cursorView.get();
        if (currPos.getLedgerId() == -1L || currPos.isEndOfLedger()) {
            readerState = ReaderState.NO_LEDGER;
        } else {
            readerState = ReaderState.PENDING_LEDGER;
            openLedgerHandle(currPos.getLedgerId(), currPos.getEntryId());
        }
    }

    public boolean hasCaughtUp() {
        return hasCaughtUp;
    }

    public void printState() {
        logger.logInfo("-------------- Log Reader state -------------");
        logger.logInfo("ReaderState: " + readerState);
        if (lh == null) {
            logger.logInfo("No ledger handle");
        } else {
            lh.printState();
        }
        logger.logInfo("---------------------------------------------");
    }

    public boolean read() {
        return openNextLedger()
            || reachedEndOfLedger()
            || longPoll()
            || readUpToLac()
            || updateCachedLedgerMetadata();
    }

    private boolean openNextLedger() {
        if (readerState == ReaderState.NO_LEDGER) {
            long currentLedgerId = cursorView.get().getLedgerId();
            long ledgerToOpen = findNextLedgerId(currentLedgerId);

            if (ledgerToOpen == -1L) {
                readerState = ReaderState.PENDING_LEDGER;
                metadataManager.getLedgerList()
                        .thenApply(this::checkForCancellation)
                        .whenComplete((Versioned<List<Long>> vLedgerList, Throwable t) -> {
                            if (t == null) {
                                if (cachedLedgerList.getVersion() > vLedgerList.getVersion()) {
                                    logger.logInfo("Ignoring stale ledger list, will retry open next ledger operation");
                                    readerState = ReaderState.NO_LEDGER;
                                } else {
                                    cachedLedgerList = vLedgerList;
                                    long nextLedgerId = findNextLedgerId(currentLedgerId);
                                    if (nextLedgerId == -1L) {
                                        logger.logInfo("No next ledger to open yet");

                                        if (isCatchUpReader) {
                                            hasCaughtUp = true;
                                            logger.logInfo("Catch-up reader has caught up");
                                        }

                                        readerState = ReaderState.NO_LEDGER;
                                    } else {
                                        openLedgerHandle(nextLedgerId, -1L);
                                    }
                                }
                            } else if (isError(t)) {
                                logger.logError("Unable to open next ledger", t);
                                readerState = ReaderState.NO_LEDGER;
                            }
                        });
            } else {
                openLedgerHandle(ledgerToOpen, -1L);
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean reachedEndOfLedger() {
        if (readerState == ReaderState.IDLE
                && lh.getCachedLedgerMetadata().getValue().getStatus().equals(LedgerStatus.CLOSED)
                && cursorView.get().getLedgerId() == lh.getLedgerId()
                && cursorView.get().getEntryId() == lh.getCachedLedgerMetadata().getValue().getLastEntryId()) {
            readerState = ReaderState.NO_LEDGER;
            return true;
        } else {
            return false;
        }
    }

    private void openLedgerHandle(long ledgerId, long entryId) {
        ledgerManager.getLedgerMetadata(ledgerId)
                .thenApply(this::checkForCancellation)
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                    if (t == null) {
                        LedgerHandle nextLh = new LedgerHandle(mapper,
                                ledgerManager,
                                messageSender,
                                logger,
                                isCancelled,
                                vlm);
                        if (vlm.getValue().getStatus().equals(LedgerStatus.CLOSED)) {
                            nextLh.setLastAddConfirmed(vlm.getValue().getLastEntryId());
                        }

                        lh = nextLh;

                        readerState = ReaderState.IDLE;
                        logger.logInfo("Opened a ledger handle for ledger " + ledgerId + " at entry id " + entryId);

                        if (entryId == -1L) {
                            // update the cursor to indicate we are in a new ledger that has not
                            // yet been read from
                            cursorUpdater.accept(new Position(lh.getLedgerId(), -1L), null);
                        }
                    } else if (isError(t)) {
                        logger.logError("Unable to open ledger metadata for next ledger", t);
                        readerState = ReaderState.NO_LEDGER;
                    }
                });
    }

    private long findNextLedgerId(long currentLedgerId) {
        if (cachedLedgerList.getValue().isEmpty()) {
            return -1L;
        } else if (currentLedgerId == -1L) {
            return cachedLedgerList.getValue().get(0);
        } else {

            boolean takeNext = false;
            for (Long ledgerId : cachedLedgerList.getValue()) {
                if (takeNext) {
                    return ledgerId;
                } else if (ledgerId == currentLedgerId) {
                    takeNext = true;
                }
            }

            return -1L;
        }
    }

    private boolean shouldUpdateCachedLedgerMetadata() {
        return readerState == ReaderState.READING
                && !pendingMetadata
                && Duration.between(lastCheckedMetadata, Instant.now()).toMillis()
                        > Constants.KvStore.ReaderUpdateMetadataIntervalMs;
    }

    private boolean updateCachedLedgerMetadata() {
        if (shouldUpdateCachedLedgerMetadata()) {
            lastCheckedMetadata = Instant.now();
            pendingMetadata = true;
            metadataManager.getLedgerList()
                    .thenApply(this::checkForCancellation)
                    .thenAccept((Versioned<List<Long>> vLedgerList) -> {
                        if (cachedLedgerList.getVersion() < vLedgerList.getVersion()) {
                            cachedLedgerList = vLedgerList;
                        }

                        if (cursorView.get().getLedgerId() > -1L) {
                            updateCurrentLedgerMetadata(cursorView.get().getLedgerId());
                        } else {
                            pendingMetadata = false;
                        }
                    })
                    .whenComplete((Void v, Throwable t) -> {
                        if (isError(t)) {
                            logger.logError("Failed updating the ledger metadata cache.", t);
                        }
                    });

            return true;
        } else {
            return false;
        }
    }

    private void updateCurrentLedgerMetadata(long ledgerId) {
        ledgerManager.getLedgerMetadata(ledgerId)
                .thenApply(this::checkForCancellation)
                .thenAccept((Versioned<LedgerMetadata> vlm) -> {
                    if (readerState == ReaderState.READING
                            || readerState == ReaderState.IN_LONG_POLL
                            || readerState == ReaderState.IDLE) {
                        LedgerMetadata cachedMd = lh.getCachedLedgerMetadata().getValue();
                        long cachedMdVersion = lh.getCachedLedgerMetadata().getVersion();

                        if (vlm.getValue().getLedgerId() == cachedMd.getLedgerId()) {
                            // we only care about ledger metadata changes that pertain to the current ledger being read
                            if (vlm.getVersion() > cachedMdVersion) {
                                // we only care about changes that have happened after our cached version
                                lh.setCachedLedgerMetadata(vlm);

                                if (readerState == ReaderState.IDLE
                                        && vlm.getValue().getStatus() == LedgerStatus.CLOSED
                                        && cursorView.get().getEntryId() == vlm.getValue().getLastEntryId()) {
                                    logger.logDebug("LedgerStatus update: ledger closed and cursor at end of the ledger."
                                            + " Changing to reader state: " + ReaderState.NO_LEDGER);
                                    readerState = ReaderState.NO_LEDGER;
                                }
                            }
                        }
                    }
                })
                .whenComplete((Void v, Throwable t) -> {
                    if (isError(t)) {
                        logger.logError("Unable to retrieve latest ledger metadata", t);
                    }
                    pendingMetadata = false;
                });
    }

    private boolean longPoll() {
        if (readerState == ReaderState.IDLE
                && !ledgerIsClosed()
                && cursorView.get().getEntryId() == lh.getLastAddConfirmed()) {
            readerState = ReaderState.IN_LONG_POLL;
            long previousLac = lh.getLastAddConfirmed();

            lh.lacLongPollRead()
                    .thenApply(this::checkForCancellation)
                    .thenAccept((Result<Entry> lacResult) -> {
                        if (readerState != ReaderState.IN_LONG_POLL) {
                            logger.logDebug("READER: ignoring stale long poll result");
                            return;
                        }

                        if (lacResult.getCode().equals(ReturnCodes.OK)) {
                            logger.logDebug("READER: Read lac " + lacResult.getData());
                            long latestLac = lacResult.getData().getLac();

                            if (latestLac > previousLac && lacResult.getData().getValue() != null) {
                                logger.logDebug("READER: Long poll has advanced the LAC from: "
                                        + previousLac + " to: " + latestLac);
                                updateCursor(lacResult.getData());
                            } else if (lacResult.getData().getLac() == previousLac) {
                                // the LAC has not advanced

                                if (lh.getCachedLedgerMetadata().getValue().getStatus() == LedgerStatus.CLOSED) {
                                    logger.logDebug("READER: Already reached end of ledger");
                                    readerState = ReaderState.NO_LEDGER;
                                } else {
                                    logger.logDebug("READER: No entries to read right now");
                                }
                            } else {
                                logger.logDebug("READER: ignoring stale long poll result");
                            }
                        } else {
                            logger.logDebug("READER: Long poll non-success result. Code=" + lacResult.getCode());
                        }
                    })
                    .whenComplete((Void v, Throwable t) -> {
                        if (isError(t)) {
                            logger.logError("Long poll failed", t);
                            readerState = ReaderState.IDLE;
                        } else if (readerState == ReaderState.IN_LONG_POLL) {
                            readerState = ReaderState.IDLE;
                        }
                    });
            return true;
        } else {
            return false;
        }
    }

    private boolean readUpToLac() {
        if (readerState == ReaderState.IDLE
                && cursorView.get().getEntryId() < lh.getLastAddConfirmed()) {
            Position lastPositionRead = cursorView.get();
            readerState = ReaderState.READING;

            readNext(lh, lastPositionRead)
                    .thenApply(this::checkForCancellation)
                    .thenAccept((Position finalPositionRead) -> {
                        logger.logDebug("Read position=" + finalPositionRead);
                        if (finalPositionRead.isEndOfLedger()) {
                            readerState = ReaderState.NO_LEDGER;
                        } else {
                            readerState = ReaderState.IDLE;
                        }
                    })
                    .whenComplete((Void v, Throwable t) -> {
                        if (isError(t)){
                            logger.logError("Failed reading ledger", t);
                            readerState = ReaderState.IDLE;
                        }
                    });
            return true;
        } else {
            return false;
        }
    }

    private CompletableFuture<Position> readNext(LedgerHandle lh, Position prev) {
        if (ledgerIsClosed() && prev.getEntryId() == lh.getCachedLedgerMetadata().getValue().getLastEntryId()) {
            logger.logDebug("READER: Reached end of the ledger");
            prev.setEndOfLedger(true);
            return CompletableFuture.completedFuture(prev);
        } else if (lh.getLastAddConfirmed() == prev.getEntryId()) {
            logger.logDebug("READER: Reached LAC");
            // we can't safely read any further
            return CompletableFuture.completedFuture(prev);
        }

        return lh.read(prev.getEntryId() + 1)
                .thenApply(this::checkForCancellation)
                .thenCompose((Result<Entry> result) -> {
                    if (result.getCode().equals(ReturnCodes.OK)) {
                        logger.logDebug("READER: Read entry success: " + result.getData().getEntryId());
                        Position pos = updateCursor(result.getData());
                        return readNext(lh, pos);
                    } else if (result.getCode().equals(ReturnCodes.Bookie.NO_SUCH_ENTRY)
                            || result.getCode().equals(ReturnCodes.Bookie.NO_SUCH_LEDGER)) {
                        logger.logError("READER: Entry may be lost. Have not reached LAC but all bookies report negatively.");
                        // we don't advance
                        return CompletableFuture.completedFuture(prev);
                    } else {
                        logger.logDebug("READER: Read inconclusive. Bookies may be unavailable.");
                        // we don't advance
                        return CompletableFuture.completedFuture(prev);
                    }
                });
    }

    private Position updateCursor(Entry entry) {
        Op op = Op.stringToOp(entry.getValue());
        Position pos = new Position(entry.getLedgerId(), entry.getEntryId());
        cursorUpdater.accept(pos, op);

        return pos;
    }

    private boolean ledgerIsClosed() {
        return lh.getCachedLedgerMetadata().getValue().getStatus() == LedgerStatus.CLOSED;
    }
}
