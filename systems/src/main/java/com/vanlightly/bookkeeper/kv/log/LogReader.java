package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.Constants;
import com.vanlightly.bookkeeper.Logger;
import com.vanlightly.bookkeeper.MessageSender;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.Entry;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerHandle;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;
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

public class LogReader extends LogClient {
    private Position readCursor;
    private LedgerHandle lh;
    private ReaderState readerState;

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
                     boolean isCatchUpReader) {
        super(metadataManager, ledgerManager, mapper, logger,
                messageSender, cursorUpdater);

        this.lastCheckedMetadata = Instant.now().minus(1, ChronoUnit.DAYS);
        this.pendingMetadata = false;
        this.isCatchUpReader = isCatchUpReader;
        this.cachedLedgerList = new Versioned<>(new ArrayList<>(), -1);
    }

    public void start(Position cursor) {
        readCursor = cursor;
        readerState = ReaderState.NO_LEDGER;
    }

    public boolean hasCaughtUp() {
        return hasCaughtUp;
    }

    public boolean read() {
        return openNextLedger()
            || readUpToLac()
            || updateCachedLedgerMetadata();
    }

    private boolean openNextLedger() {
        if (readerState == ReaderState.NO_LEDGER) {
            long currentLedgerId = readCursor.getLedgerId();
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
                    .thenCompose((Versioned<List<Long>> vLedgerList) -> {
                        if (cachedLedgerList.getVersion() < vLedgerList.getVersion()) {
                            cachedLedgerList = vLedgerList;
                        }
                        return ledgerManager.getLedgerMetadata(readCursor.getLedgerId());
                    })
                    .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                        if (t != null) {
                            logger.logError("Unable to retrieve latest ledger metadata", t);
                        } else {
                            if (readerState == ReaderState.READING || readerState == ReaderState.IDLE) {
                                LedgerMetadata cachedMd = lh.getCachedLedgerMetadata().getValue();
                                long cachedMdVersion = lh.getCachedLedgerMetadata().getVersion();

                                if (vlm.getValue().getLedgerId() == cachedMd.getLedgerId()) {
                                    // we only care about ledger metadata changes that pertain to the current ledger being read
                                    if (vlm.getVersion() > cachedMdVersion) {
                                        // we only care about changes that have happened after our cached version
                                        lh.setCachedLedgerMetadata(vlm);
                                    }
                                }
                            }
                        }
                        pendingMetadata = false;
                    });
            return true;
        } else {
            return false;
        }
    }

    private boolean readUpToLac() {
        if (readerState == ReaderState.IDLE) {
            Position lastPositionRead = readCursor;
            readerState = ReaderState.READING;

            lh.readExplicitLac()
                    .thenCompose((Long lac) -> {
                        logger.logDebug("READER: Read lac " + lac);
                        if (lac == lastPositionRead.getEntryId()) {
                            if (lh.getCachedLedgerMetadata().getValue().getStatus() == LedgerStatus.CLOSED) {
                                logger.logDebug("READER: Reached end of ledger");
                                Position endOfLedgerPos = new Position(lastPositionRead.getLedgerId(),
                                        lastPositionRead.getEntryId(), true);
                                return CompletableFuture.completedFuture(endOfLedgerPos);
                            } else {
                                logger.logDebug("READER: No entries to read right now");
                                return CompletableFuture.completedFuture(lastPositionRead);
                            }
                        } else {
                            Position next = new Position(lastPositionRead.getLedgerId(),
                                    lastPositionRead.getEntryId() + 1);
                            return readNext(lh, next);
                        }
                    })
                    .whenComplete((Position finalPositionRead, Throwable t) -> {
                        if (t != null) {
                            logger.logError("Failed reading ledger", t);
                            readerState = ReaderState.IDLE;
                        } else {
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
        if (lh.getLastAddConfirmed() == prev.getEntryId()) {
            logger.logDebug("READER: Reached LAC");
            // we can't safely read any further
            return CompletableFuture.completedFuture(prev);
        }

        return lh.read(prev.getEntryId() + 1)
                .thenCompose((Entry entry) -> {
                    logger.logDebug("READER: Read entry " + entry.getEntryId());

                    // add to kv store
                    Op op = Op.stringToOp(entry.getValue());
                    Position pos = new Position(entry.getLedgerId(), entry.getEntryId());
                    cursorUpdater.accept(pos, op);

                    // update position
                    readCursor = pos;

                    // read the next entry
                    return readNext(lh, pos);
                });
    }
}
