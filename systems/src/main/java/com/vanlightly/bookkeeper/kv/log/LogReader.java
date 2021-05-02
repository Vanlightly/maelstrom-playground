package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.*;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.LedgerStatus;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.util.InvariantViolationException;
import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/*
    Reads the log and updates the cursor (and KV store).
    Abstracts away ledgers and handles - is at the log abstraction level.

    A reader instance can read from multiple ledgers, sequentially,
    and has more concurrency than the writer. While it is performing read
    operations, it is also polling the metadata in order to keep up
    with changes to the ledger list. For this reason it maintains more state
    machine housekeeping than the writer.
 */
public class LogReader extends LogClient {
    private Logger logger = LogManager.getLogger(this.getClass().getName());
    private LedgerReadHandle readHandle;
    private ReaderSM sm;
    private Supplier<Position> cursorView;

    private Instant lastCheckedMetadata;
    private boolean pendingMetadata;
    private Versioned<List<Long>> cachedLedgerList;

    private boolean isCatchUpReader;
    private boolean hasCaughtUp;
    private long upToLedger;

    public LogReader(ManagerBuilder builder,
                     ObjectMapper mapper,
                     MessageSender messageSender,
                     BiConsumer<Position, Op> cursorUpdater,
                     Supplier<Position> cursorView,
                     boolean isCatchUpReader,
                     long upToLedger) {
        super(builder, mapper, messageSender, cursorUpdater);

        this.lastCheckedMetadata = Instant.now().minus(1, ChronoUnit.DAYS);
        this.pendingMetadata = false;
        this.isCatchUpReader = isCatchUpReader;
        this.cachedLedgerList = new Versioned<>(new ArrayList<>(), -1);
        this.cursorView = cursorView;
        this.upToLedger = upToLedger;
    }

    @Override
    public void cancel() {
        isCancelled.set(true);
        if (readHandle != null) {
            readHandle.cancel();
        }
    }

    public void start() {
        Position currPos = cursorView.get();
        if (currPos.getLedgerId() == -1L || currPos.isEndOfLedger()) {
            sm = new ReaderSM(logger, State.NO_LEDGER);
        } else {
            sm = new ReaderSM(logger, State.PENDING_LEDGER);;
            openLedgerHandle(currPos.getLedgerId(), currPos.getEntryId());
        }
    }

    public boolean hasCaughtUp() {
        return hasCaughtUp;
    }

    public void printState() {
        logger.logInfo("-------------- Log Reader state -------------");
        logger.logInfo("ReaderState: " + sm.state);
        logger.logInfo("Cursor: Ledger=" + cursorView.get().getLedgerId()
            + " Entry=" + cursorView.get().getEntryId()
            + " At end of ledger=" + cursorView.get().isEndOfLedger());
        if (readHandle == null) {
            logger.logInfo("No ledger read handle");
        } else {
            readHandle.printState();
        }
        logger.logInfo("---------------------------------------------");
    }

    public boolean read() {
        checkInvariants();

        return openNextLedger()
                || longPoll()
                || readUpToLac()
                || reachedEndOfLedger()
                || updateCachedLedgerMetadata()
                || isInIllegalState();
    }

    private boolean openNextLedger() {
        if (sm.state == State.NO_LEDGER
                && !hasCaughtUp) {
            long currentLedgerId = cursorView.get().getLedgerId();
            long ledgerToOpen = findNextLedgerId(currentLedgerId);

            if (isCatchUpReader && currentLedgerId == upToLedger) {
                hasCaughtUp = true;
                logger.logInfo("Catch-up reader has caught up to the closed ledger: " + upToLedger);
                sm.changeState(State.CLOSED);
                return true;
            }

            sm.changeState(State.PENDING_LEDGER);

            if (ledgerToOpen == -1L) {
                metadataManager.getLedgerList()
                        .thenApply(this::checkForCancellation)
                        .whenComplete((Versioned<List<Long>> vLedgerList, Throwable t) -> {
                            if (t == null) {
                                if (cachedLedgerList.getVersion() > vLedgerList.getVersion()) {
                                    logger.logInfo("Ignoring stale ledger list, will retry open next ledger operation");
                                    sm.changeState(State.NO_LEDGER);
                                } else {
                                    cachedLedgerList = vLedgerList;
                                    long nextLedgerId = findNextLedgerId(currentLedgerId);
                                    if (nextLedgerId == -1L) {
                                        logger.logInfo("No next ledger to open yet");

                                        sm.changeState(State.NO_LEDGER);
                                    } else {
                                        openLedgerHandle(nextLedgerId, -1L);
                                    }
                                }
                            } else if (isError(t)) {
                                logger.logError("Unable to open next ledger", t);
                                sm.changeState(State.NO_LEDGER);
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
        if (sm.state == State.IDLE
                && ledgerIsClosed()
                && cursorView.get().getLedgerId() == readHandle.getLedgerId()
                && cursorView.get().getEntryId() == lm().getLastEntryId()) {
            sm.changeState(State.NO_LEDGER);
            logger.logDebug("Reader has reached the end of ledger " + cursorView.get().getLedgerId()
                + " entry " + cursorView.get().getEntryId());
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
                        LedgerReadHandle nextLrh = new LedgerReadHandle(mapper,
                                ledgerManager,
                                messageSender,
                                vlm);
                        if (vlm.getValue().getStatus().equals(LedgerStatus.CLOSED)) {
                            nextLrh.setLastAddConfirmed(vlm.getValue().getLastEntryId());
                        }

                        if (readHandle != null) {
                            // cancel any operations just in case
                            readHandle.cancel();
                        }
                        readHandle = nextLrh;

                        sm.changeState(State.IDLE);
                        logger.logInfo("Opened a ledger read handle for ledger " + ledgerId + " at entry id " + entryId);

                        if (entryId == -1L) {
                            // update the cursor to indicate we are in a new ledger that has not
                            // yet been read from
                            cursorUpdater.accept(new Position(readHandle.getLedgerId(), -1L), null);
                        } else {
                            readHandle.setLastAddConfirmed(entryId);
                        }
                    } else if (isError(t)) {
                        logger.logError("Unable to open ledger metadata for next ledger", t);
                        sm.changeState(State.NO_LEDGER);
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
                    if (isCatchUpReader && upToLedger > -1L && ledgerId > upToLedger) {
                        throw new BkException("The catch-up log reader has reached a ledger that should not exist.", ReturnCodes.Ledger.UNEXPECTED_LEDGER);
                    }

                    return ledgerId;
                } else if (ledgerId == currentLedgerId) {
                    takeNext = true;
                }
            }

            return -1L;
        }
    }

    private boolean shouldUpdateCachedLedgerMetadata() {
        return sm.state == State.READING
                && !pendingMetadata
                && Duration.between(lastCheckedMetadata, Instant.now()).toMillis()
                        > Constants.KvStore.ReaderUpdateMetadataIntervalMs;
    }

    private boolean updateCachedLedgerMetadata() {
        if (shouldUpdateCachedLedgerMetadata()) {
            lastCheckedMetadata = Instant.now();
            pendingMetadata = true;
            final int stateCtr = sm.getStateCtr();
            metadataManager.getLedgerList()
                    .thenApply(this::checkForCancellation)
                    .thenAccept((Versioned<List<Long>> vLedgerList) -> {
                        if (sm.getStateCtr() == stateCtr) {
                            if (cachedLedgerList.getVersion() < vLedgerList.getVersion()) {
                                cachedLedgerList = vLedgerList;
                            }

                            if (cursorView.get().getLedgerId() > -1L) {
                                updateCurrentLedgerMetadata(cursorView.get().getLedgerId());
                            } else {
                                pendingMetadata = false;
                            }
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
                    if (sm.state == State.READING
                            || sm.state == State.IN_LONG_POLL
                            || sm.state == State.IDLE) {
                        LedgerMetadata cachedMd = readHandle.getCachedLedgerMetadata().getValue();
                        long cachedMdVersion = readHandle.getCachedLedgerMetadata().getVersion();

                        if (vlm.getValue().getLedgerId() == cachedMd.getLedgerId()) {
                            // we only care about ledger metadata changes that pertain to the current ledger being read
                            if (vlm.getVersion() > cachedMdVersion) {
                                // we only care about changes that have happened after our cached version
                                readHandle.setCachedLedgerMetadata(vlm);

                                if (sm.state == State.IDLE
                                        && vlm.getValue().getStatus() == LedgerStatus.CLOSED
                                        && cursorView.get().getEntryId() == vlm.getValue().getLastEntryId()) {
                                    logger.logDebug("LedgerStatus update: ledger closed and cursor at end of the ledger."
                                            + " Changing to reader state: " + State.NO_LEDGER);
                                    sm.changeState(State.NO_LEDGER);
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
        if (sm.state == State.IDLE
                && !ledgerIsClosed()
                && cursorView.get().getEntryId() == readHandle.getLastAddConfirmed()) {
            final int stateCtr = sm.changeState(State.IN_LONG_POLL);
            long previousLac = readHandle.getLastAddConfirmed();

            logger.logDebug("Sending long poll with previousLac: " + previousLac + " ledger: " + readHandle.getLedgerId());
            readHandle.lacLongPollRead()
                    .thenApply(this::checkForCancellation)
                    .thenAccept((Result<Entry> lacResult) -> {
                        if (!sm.isInState(stateCtr)) {
                            logger.logDebug("ignoring stale long poll result");
                            return;
                        }

                        if (lacResult.getCode().equals(ReturnCodes.OK)) {
                            logger.logDebug("Long poll read lac " + lacResult.getData());
                            long latestLac = lacResult.getData().getLac();

                            if (latestLac > previousLac && lacResult.getData().getValue() != null) {
                                logger.logDebug("Long poll has advanced the LAC from: "
                                        + previousLac + " to: " + latestLac);
                                updateCursor(lacResult.getData());
                            } else if (lacResult.getData().getLac() == previousLac) {
                                // the LAC has not advanced

                                if (lm().getStatus() == LedgerStatus.CLOSED) {
                                    if (lacResult.getData().getEntryId() == lm().getLastEntryId()) {
                                        logger.logDebug("Ledger closed and have reached the end of the ledger");
                                        sm.changeState(State.NO_LEDGER);
                                    } else {
                                        logger.logDebug("Ledger closed, switching to regular reads");
                                        sm.changeState(State.IDLE);
                                    }
                                } else {
                                    logger.logDebug("No entries to read right now");
                                }
                            } else {
                                logger.logDebug("ignoring stale long poll result");
                            }
                        } else {
                            logger.logDebug("Long poll non-success result. Code=" + lacResult.getCode());
                        }
                    })
                    .whenComplete((Void v, Throwable t) -> {
                        if (isError(t)) {
                            logger.logError("Long poll failed", t);
                        }

                        if (sm.state == State.IN_LONG_POLL) {
                            sm.changeState(State.IDLE);
                        }
                    });
            return true;
        } else {
            return false;
        }
    }

    private boolean readUpToLac() {
        if (sm.state == State.IDLE
                && ((!ledgerIsClosed() && cursorView.get().getEntryId() < readHandle.getLastAddConfirmed())
                    || (ledgerIsClosed() && cursorView.get().getEntryId() < lm().getLastEntryId()))) {
            Position lastPositionRead = cursorView.get();
            final int stateCtr = sm.changeState(State.READING);

            readNext(lastPositionRead, stateCtr)
                    .thenApply(this::checkForCancellation)
                    .thenAccept((Position finalPositionRead) -> {
                        if (!sm.isInState(stateCtr)) {
                            logger.logDebug("Ignoring stale read result");
                            return;
                        }

                        logger.logDebug("Read position=" + finalPositionRead);
                        if (finalPositionRead.isEndOfLedger()) {
                            sm.changeState(State.NO_LEDGER);
                        } else {
                            sm.changeState(State.IDLE);
                        }
                    })
                    .whenComplete((Void v, Throwable t) -> {
                        if (isError(t)){
                            logger.logError("Failed reading ledger", t);
                            sm.changeState(State.IDLE);
                        }
                    });
            return true;
        } else {
            return false;
        }
    }

    private CompletableFuture<Position> readNext(Position prev, final int stateCtr) {
        if (!sm.isInState(stateCtr)) {
            logger.logDebug("Ignoring read request due to state change");
            return CompletableFuture.completedFuture(prev);
        } else if (ledgerIsClosed() && prev.getEntryId() == lm().getLastEntryId()) {
            logger.logDebug("Reached end of closed ledger: " + lm().getLedgerId());
            prev.setEndOfLedger(true);
            return CompletableFuture.completedFuture(prev);
        } else if (!ledgerIsClosed() && prev.getEntryId() == readHandle.getLastAddConfirmed()) {
            logger.logDebug("Reached LAC of open ledger: " + lm().getLedgerId());
            // we can't safely read any further
            return CompletableFuture.completedFuture(prev);
        }

        return readHandle.read(prev.getEntryId() + 1)
                .thenApply(this::checkForCancellation)
                .thenCompose((Result<Entry> result) -> {
                    if (!sm.isInState(stateCtr)) {
                        logger.logDebug("Ignoring read result due to state change");
                        return CompletableFuture.completedFuture(prev);
                    } else if (result.getCode().equals(ReturnCodes.OK)) {
                        if (result.getData().getValue().equals("")) {
                            logger.logError("Entry read has no value: " + result.getData());
                        }

                        logger.logDebug("Read entry success: " + result.getData());
                        Position pos = updateCursor(result.getData());
                        return readNext(pos, stateCtr);
                    } else if (result.getCode().equals(ReturnCodes.Bookie.NO_SUCH_ENTRY)
                            || result.getCode().equals(ReturnCodes.Bookie.NO_SUCH_LEDGER)) {
                        logger.logError("Entry may be lost. Have not reached LAC but all bookies report negatively.");
                        // we don't advance
                        return CompletableFuture.completedFuture(prev);
                    } else {
                        logger.logDebug("Read inconclusive. Bookies may be unavailable.");
                        // we don't advance
                        return CompletableFuture.completedFuture(prev);
                    }
                });
    }

    private boolean isInIllegalState() {
        if (sm.state == State.IDLE) {
            logger.logInvariantViolation("Cannot be in IDLE state without matching action", "TEMP");
            printState();
            throw new InvariantViolationException("Cannot be in IDLE state without matching action");
        } else {
            return false;
        }
    }

    private Position updateCursor(Entry entry) {
        Op op = Op.stringToOp(entry.getValue());
        Position pos = new Position(entry.getLedgerId(), entry.getEntryId());
        cursorUpdater.accept(pos, op);

        return pos;
    }

    private boolean ledgerIsClosed() {
        return lm().getStatus() == LedgerStatus.CLOSED;
    }

    private LedgerMetadata lm() {
        return readHandle.getCachedLedgerMetadata().getValue();
    }

    private void checkInvariants() {
        checkLedgerTruncated();
    }

    private void checkLedgerTruncated() {
        if ((sm.state == State.IDLE
                || sm.state == State.IN_LONG_POLL
                || sm.state == State.READING)
            && lm().getStatus() == LedgerStatus.CLOSED
            && cursorView.get().getEntryId() > lm().getLastEntryId()) {

            logger.logInvariantViolation("Reader cursor is ahead of the last entry id. "
                    + " Cursor at:" + cursorView.get().getEntryId()
                    + " Ledger closed at: " + lm().getLastEntryId(), Invariants.LOG_TRUNCATION);
            printState();
            throw new InvariantViolationException("Ledger truncated");
        }
    }

    private enum State {
        NO_LEDGER,
        PENDING_LEDGER,
        READING,
        IN_LONG_POLL,
        IDLE,
        CLOSED
    }

    // Reader state machine. Counters indicate when asynchronous results are stale.
    private static class ReaderSM {

        private Logger logger;
        private AtomicInteger stateCounter;
        private State state;

        public ReaderSM(Logger logger, State state) {
            this.logger = logger;
            this.state = state;
            stateCounter = new AtomicInteger(0);
        }

        public int getStateCtr() {
            return stateCounter.get();
        }

        public boolean isInState(int stateCtr) {
            return stateCounter.get() == stateCtr;
        }

        public int changeState(State rState) {
            state = rState;
            return stateCounter.incrementAndGet();
        }
    }
}
