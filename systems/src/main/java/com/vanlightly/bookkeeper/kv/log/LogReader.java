package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.Constants;
import com.vanlightly.bookkeeper.Logger;
import com.vanlightly.bookkeeper.MessageSender;
import com.vanlightly.bookkeeper.ReturnCodes;
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

public class LogReader extends LogClient {
    private LedgerHandle lh;
    private ReaderState readerState;
    private Supplier<Position> cursorView;

    private Instant lastCheckedMetadata;
    private boolean pendingMetadata;
    private Versioned<List<Long>> cachedLedgerList;

    private boolean isCatchUpReader;
    private boolean hasCaughtUp;

    public LogReader(MetadataManager metadataManager,
                     LedgerManager ledgerManager,
                     ObjectMapper mapper,
                     Logger logger,
                     MessageSender messageSender,
                     BiConsumer<Position, Op> cursorUpdater,
                     Supplier<Position> cursorView,
                     boolean isCatchUpReader) {
        super(metadataManager, ledgerManager, mapper, logger,
                messageSender, cursorUpdater);

        this.lastCheckedMetadata = Instant.now().minus(1, ChronoUnit.DAYS);
        this.pendingMetadata = false;
        this.isCatchUpReader = isCatchUpReader;
        this.cachedLedgerList = new Versioned<>(new ArrayList<>(), -1);
        this.cursorView = cursorView;
    }

    public void start() {
        readerState = ReaderState.NO_LEDGER;
    }

    public boolean hasCaughtUp() {
        return hasCaughtUp;
    }

    public boolean read() {
        return openNextLedger()
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
                        .whenComplete((Versioned<List<Long>> vLedgerList, Throwable t) -> {
                            if (t != null) {
                                logger.logError("Unable to open next ledger", t);
                                readerState = ReaderState.NO_LEDGER;
                            } else if (cachedLedgerList.getVersion() > vLedgerList.getVersion()) {
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
                                    openLedgerHandle(nextLedgerId);
                                }
                            }
                        });
            } else {
                openLedgerHandle(ledgerToOpen);
            }
            return true;
        } else {
            return false;
        }
    }

    private void openLedgerHandle(long ledgerId) {
        ledgerManager.getLedgerMetadata(ledgerId)
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t2) -> {
                    if (t2 != null) {
                        logger.logError("Unable to open ledger metadata for next ledger", t2);
                        readerState = ReaderState.NO_LEDGER;
                    } else {
                        logger.logInfo("Opened a ledger handle for the next ledger");
                        LedgerHandle nextLh = new LedgerHandle(mapper,
                                ledgerManager,
                                messageSender,
                                logger,
                                isCancelled,
                                vlm);
                        this.lh = nextLh;
                        readerState = ReaderState.IDLE;
                        cursorUpdater.accept(new Position(lh.getLedgerId(), -1L), null);
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
                    .thenAccept((Versioned<List<Long>> vLedgerList) -> {
                        if (cachedLedgerList.getVersion() < vLedgerList.getVersion()) {
                            cachedLedgerList = vLedgerList;
                        }

                        if (cursorView.get().getLedgerId() > -1L) {
                            updateCurrentLedgerMetadata(cursorView.get().getLedgerId());
                        } else {
                            pendingMetadata = false;
                        }
                    });

            return true;
        } else {
            return false;
        }
    }

    private void updateCurrentLedgerMetadata(long ledgerId) {
        ledgerManager.getLedgerMetadata(ledgerId)
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                    if (t != null) {
                        logger.logError("Unable to retrieve latest ledger metadata", t);
                    } else {
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
                    .thenAccept((Result<Entry> lacResult) -> {
                        if (readerState != ReaderState.IN_LONG_POLL) {
                            logger.logDebug("READER: ignoring stale long poll result");
                            return;
                        }

                        if (lacResult.getCode().equals(ReturnCodes.OK)) {
                            logger.logDebug("READER: Read lac " + lacResult.getData());
                            long latestLac = lacResult.getData().getLac();

                            if (latestLac > previousLac) {
                                updateCursor(lacResult);
                                readerState = ReaderState.IDLE;
                            } else if (lacResult.getData().getLac() == previousLac) {
                                // the LAC has not advanced

                                if (lh.getCachedLedgerMetadata().getValue().getStatus() == LedgerStatus.CLOSED) {
                                    logger.logDebug("READER: Reached end of ledger");
                                    readerState = ReaderState.NO_LEDGER;
                                } else {
                                    logger.logDebug("READER: No entries to read right now");
                                    readerState = ReaderState.IDLE;
                                }
                            } else {
                                logger.logDebug("READER: ignoring stale long poll result");
                            }
                        } else {
                            logger.logDebug("READER: Long poll non-success result. Code=" + lacResult.getCode());
                        }
                    })
                    .whenComplete((Void v, Throwable t) -> {
                        if (t != null) {
                            logger.logError("Long poll failed", t);
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
                    .whenComplete((Position finalPositionRead, Throwable t) -> {
                        if (t != null) {
                            logger.logError("Failed reading ledger", t);
                            readerState = ReaderState.IDLE;
                        } else {
                            logger.logDebug("Read position=" + finalPositionRead);
                            if (finalPositionRead.isEndOfLedger()) {
                                readerState = ReaderState.NO_LEDGER;
                            } else {
                                readerState = ReaderState.IDLE;
                            }
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
                .thenCompose((Result<Entry> result) -> {
                    if (result.getCode().equals(ReturnCodes.OK)) {
                        logger.logDebug("READER: Read entry success: " + result.getData().getEntryId());
                        Position pos = updateCursor(result);
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

    private Position updateCursor(Result<Entry> result) {
        Op op = Op.stringToOp(result.getData().getValue());
        Position pos = new Position(result.getData().getLedgerId(), result.getData().getEntryId());
        cursorUpdater.accept(pos, op);

        return pos;
    }

    private boolean ledgerIsClosed() {
        return lh.getCachedLedgerMetadata().getValue().getStatus() == LedgerStatus.CLOSED;
    }
}
