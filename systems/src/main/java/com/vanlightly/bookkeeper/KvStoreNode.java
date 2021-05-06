package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.kv.*;
import com.vanlightly.bookkeeper.kv.bkclient.BkException;
import com.vanlightly.bookkeeper.kv.log.*;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.network.NetworkIO;
import com.vanlightly.bookkeeper.util.Futures;
import com.vanlightly.bookkeeper.util.InvariantViolationException;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class KvStoreNode extends Node {
    private MetadataManager metadataManager;
    private LogWriter logWriter;
    private LogReader logReader;
    private LogSegmentCloser logSegmentCloser;
    private KvStore kvStore;
    private OpLog opLog;

    private KvStoreState state;
    private Position cursor;
    private Versioned<String> cachedLeaderId;
    private Position closedAtPosition;
    private Versioned<List<Long>> lastLedgerList;

    private Instant lastUpdatedMetadata;
    private boolean pendingLeaderResult;

    /*
        If no KV commands are being received, then NoOp ops are replicated
        in order to advance the LAC (hmm is this what explicit LAC is for?)
     */
    private Instant lastAppendedNoOp;
    private Position lastPosOfNoOpCheck;

    public KvStoreNode(String nodeId,
                       NetworkIO net,
                       ManagerBuilder builder) {
        super(nodeId, NodeType.KvStore,true, net, builder);
        this.metadataManager = builder.buildMetadataManager(this, isCancelled);
        this.lastUpdatedMetadata = Instant.now().minus(1, ChronoUnit.DAYS);
        this.kvStore = new KvStore();
        this.opLog = new OpLog();
        this.state = new KvStoreState();
        this.cursor = new Position(-1L, -1L);
        this.lastPosOfNoOpCheck = new Position(-1L, -1L);
        this.lastAppendedNoOp = Instant.now().minus(1, ChronoUnit.DAYS);
        this.cachedLeaderId = new Versioned<>(Constants.Metadata.NoLeader, -1);
        this.closedAtPosition = new Position(-1, -1);
    }


    @Override
    public void initialize(JsonNode initMsg) {
        sendInitOk(initMsg);
    }

    @Override
    public boolean roleSpecificAction() {
        return sessionManager.maintainSession()
                || checkLeadership()
                || initiateNewWriterSequence()
                || closeLastLogSegment()
                || startCatchUpReader()
                || keepCatchingUp()
                || startWriter()
                || writerSegmentNoLongerOpen()
                || replicate()
                || applyOp()
                || appendNoOp()
                || startReader()
                || keepReading();
    }

    private boolean shouldCheckLeadership() {
        // if we're pending a leader check result, then no
        if (pendingLeaderResult) {
            return false;
        }

        // else if we haven't yet got a role then yes
        if (state.role == KvStoreState.Role.NONE) {
            return true;
        }

        // else if we're not in the ready state then no
        if (!leaderIs(KvStoreState.LeaderState.READY) && !followerIs(KvStoreState.FollowerState.READING)) {
            return false;
        }

        // else if its been longer than the check interval then yes
        return Duration.between(lastUpdatedMetadata, Instant.now()).toMillis() > Constants.KvStore.CheckLeadershipIntervalMs;
    }

    private boolean checkLeadership() {
        if (shouldCheckLeadership()) {
            pendingLeaderResult = true;
            lastUpdatedMetadata = Instant.now();
            metadataManager.getLeader()
                    .thenAccept((Versioned<String> vLeaderId) ->
                            reactToLeaderUpdate(vLeaderId))
                    .whenComplete((Void v, Throwable t) -> {
                        pendingLeaderResult = false;
                        if (t != null) {
                            logger.logError("Failed to obtain latest leader id", t);
                        }
                    });
            return true;
        } else {
            return false;
        }
    }

    private void reactToLeaderUpdate(Versioned<String> vLeaderId) {
        if (cachedLeaderId.getVersion() > vLeaderId.getVersion()) {
            logger.logInfo("Ignoring stale leader update");
            return;
        }

        switch (state.role) {
            case LEADER:
                if (cachedLeaderId.getVersion() < vLeaderId.getVersion()) {
                    logger.logDebug("New leader version. From: "
                            + cachedLeaderId.getVersion() + " to:" + vLeaderId.getVersion());
                    // the leader version is higher, a leader change has occurred

                    if (nodeId.equals(vLeaderId.getValue())) {
                        state.changeRole(KvStoreState.Role.LEADER);
                        state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_1, cursor);
                    } else {
                        // abdicate leadership
                        state.changeRole(KvStoreState.Role.FOLLOWER);
                        state.changeFollowerState(KvStoreState.FollowerState.NEED_READER, cursor);
                        state.changeLeaderState(KvStoreState.LeaderState.NONE, cursor);
                    }

                    truncateUncommittedOps("Leader change");
                    cancelAllOperations();
                }
                break;
            case FOLLOWER:
                if (cachedLeaderId.getVersion() < vLeaderId.getVersion()) {
                    if (nodeId.equals(vLeaderId.getValue())) {
                        // this node is the new leader, so cancel the reader
                        logReader.cancel();
                        logReader = null;

                        state.changeRole(KvStoreState.Role.LEADER);
                        state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_1, cursor);

                        opLog.resolveTempLog();
                    }
                }
                break;
            default:
                // first leader result
                cachedLeaderId = vLeaderId;
                if (cachedLeaderId.getValue().equals(nodeId)) {
                    state.changeRole(KvStoreState.Role.LEADER);
                    state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_1, cursor);
                } else {
                    state.changeRole(KvStoreState.Role.FOLLOWER);
                    state.changeFollowerState(KvStoreState.FollowerState.NEED_READER, cursor);
                }
                break;
        }

        cachedLeaderId = vLeaderId;
    }

    private boolean initiateNewWriterSequence() {
        if (leaderIs(KvStoreState.LeaderState.START_SEQ_1)) {
            logger.logDebug("Starting new writer sequence. Obtaining the ledger list");
            final int stateCtr = state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_2_CACHE_LEDGER_LIST, cursor);
            metadataManager.getLedgerList()
                    .thenAccept((Versioned<List<Long>> ledgerList) -> {
                        if (!state.isInState(stateCtr)) {
                            return;
                        }

                        logger.logDebug("Ledger list at start of new writer sequence: version="
                                + ledgerList.getVersion() + " list=" + ledgerList.getValue());

                        // Store the ledger list now, it will be used later when appending a new ledger.
                        // If another writer manages to append a ledger during the time between now and this writer
                        // creating its own ledger, then this writer will abdicate
                        lastLedgerList = ledgerList;
                        state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_3_CLOSE_SEGMENT, cursor);
                    })
                    .whenComplete((Void v, Throwable t) -> {
                        if (t != null && state.isInState(stateCtr)) {
                            logger.logError("Failed to cache the ledger list", t);
                            state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_1, cursor);
                        }
                    });

            return true;
        } else {
            return false;
        }
    }

    private boolean closeLastLogSegment() {
        if (leaderIs(KvStoreState.LeaderState.START_SEQ_3_CLOSE_SEGMENT)) {
            final int stateCtr = state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_4_SEGMENT_CLOSING, cursor);
            logSegmentCloser = newLogSegmentCloser();
            logSegmentCloser.closeLastSegment()
                    .whenComplete((Position closedAtPos, Throwable t) -> {
                        if (state.isInState(stateCtr)) {
                            if (t != null) {
                                if (shouldAbdicate(t)) {
                                    abdicate();
                                } else {
                                    logger.logError("Failed closing the last log segment.", t);
                                    state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_1, cursor);
                                }
                            } else {
                                logger.logInfo("Log segment closed at position: " + closedAtPos);
                                state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_5_NEED_CATCHUP_READER, cursor);
                                closedAtPosition = closedAtPos;

                                if (closedAtPosition.getLedgerId() == cursor.getLedgerId()
                                        && closedAtPosition.getEntryId() < cursor.getEntryId()) {
                                    logger.logInvariantViolation("The writer cursor is ahead of the closed segment last entry. "
                                            + " Cursor: " + cursor
                                            + " Closed at: " + closedAtPos, Invariants.LOG_TRUNCATION);
                                    throw new InvariantViolationException("The cursor is ahead of the closed segment last entry. Truncation violation.");
                                }
                            }

                            logSegmentCloser = null;
                        } else {
                            logger.logDebug("Ignoring stale completion: LogSegmentCloser::closeSegment");
                        }
                    });

            return true;
        } else {
            return false;
        }
    }

    private boolean startCatchUpReader() {
        if (leaderIs(KvStoreState.LeaderState.START_SEQ_5_NEED_CATCHUP_READER)) {
            state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_6_CATCHUP_READING, cursor);
            logReader = newCatchupLogReader(closedAtPosition.getLedgerId());
            logReader.start();
            closedAtPosition = new Position(-1, -1);
            return true;
        } else {
            return false;
        }
    }

    private boolean keepCatchingUp() {
        if (leaderIs(KvStoreState.LeaderState.START_SEQ_6_CATCHUP_READING)
                && !logReader.hasCaughtUp()) {
            try {
                return logReader.read();
            } catch (BkException bke) {
                if (bke.getCode().equals(ReturnCodes.Ledger.UNEXPECTED_LEDGER)) {
                    logger.logInfo("The catch-up reader has reached a ledger that is after the last recovered ledger. "
                        + " This occurs when another client has won leadership and appended their own ledger. Abdicating.");
                    abdicate();
                    return true;
                } else {
                    // shouldn't happen but let the node crash if it does
                    throw bke;
                }
            }
        } else {
            return false;
        }
    }

    private boolean writerSegmentNoLongerOpen() {
        if (leaderIs(KvStoreState.LeaderState.READY) && !logWriter.isHealthy()) {
            logger.logInfo("The current segment is no longer open. Abdicate.");
            abdicate();
            return true;
        } else {
            return false;
        }
    }

    private boolean startWriter() {
        if ((leaderIs(KvStoreState.LeaderState.START_SEQ_6_CATCHUP_READING) && logReader.hasCaughtUp())
                || leaderIs(KvStoreState.LeaderState.START_SEQ_7_NEED_WRITER)) {
            final int stateCtr = state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_8_STARTING_WRITER, cursor);

            logWriter = newLogWriter();
            logWriter.start(lastLedgerList)
                    .whenComplete((Void v, Throwable t) -> {
                        if (state.isInState(stateCtr)) {
                            if (t == null) {
                                state.changeLeaderState(KvStoreState.LeaderState.READY, cursor);
                                opLog.resolveTempLog();
                            } else if (shouldAbdicate(t)) {
                                logger.logDebug("The writer has aborted start-up. The cached ledger list is stale.");
                                abdicate();
                            } else {
                                // some other error occurred, we'll just try and start the writer again
                                if (t instanceof BkException) {
                                    logger.logError("Writer failed to start. Code="
                                            + ((BkException)t).getCode() + ". Will retry.");
                                } else {
                                    logger.logError("Writer failed to start due to an unexpected error. Will retry.", t);
                                }
                                logWriter.cancel();
                                logWriter = null;

                                delay(500).thenRun(() -> {
                                    if (state.isInState(stateCtr)) {
                                        state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_7_NEED_WRITER, cursor);
                                    }
                                });
                            }
                        } else {
                            logger.logDebug("Ignoring stale completion: logWriter::start");
                        }
                    });
            return true;
        } else {
            return false;
        }
    }

    public boolean replicate() {
        if (leaderIs(KvStoreState.LeaderState.READY)
                && opLog.hasUnreplicated()) {

            Op op = opLog.getNextUnreplicatedOp();
            String value = Op.opToString(op);
            logger.logDebug("Replicating op: " + value);
            final int stateCtr = state.getStateCtr();
            logWriter.write(value)
                    .whenComplete((Void v, Throwable t) -> {
                        if (t != null) {
                            // a write can only fail when there are not enough non-faulty bookies
                            if (state.isInState(stateCtr)) {
                                logger.logError("Replication failure, nacking all pending ops", t);

                                if (shouldAbdicate(t)) {
                                    abdicate();
                                } else {
                                    truncateUncommittedOps("Internal error");
                                    closeDueToReplicationError();
                                }
                            } else {
                                logger.logDebug("Ignoring stale completion: LogWriter::write");
                            }
                        }
                    });
            return true;
        } else {
            return false;
        }
    }

    private void truncateUncommittedOps(String reason) {
        // nack all pending operations
        logger.logDebug("Truncating uncommitted ops due to: " + reason);
        List<Op> uncommitedOps = opLog.truncateUncomittedOps();

        for (Op failedOp : uncommitedOps) {
            if (!failedOp.getFields().get(Fields.MSG_TYPE).equals(Constants.KvStore.Ops.NOOP)) {
                failOp(failedOp.getFields(), reason);
            }
        }
    }

    private void failOp(Map<String,String> opFields, String reason) {
        ObjectNode reply = mapper.createObjectNode();
        reply.put(Fields.IN_REPLY_TO, Integer.parseInt(opFields.get(Fields.MSG_ID)));
        reply.put("error", 14);
        reply.put("text", reason);
        send(opFields.get(Fields.SOURCE), Commands.Client.ERROR, reply);
    }

    private void closeDueToReplicationError() {
        state.changeLeaderState(KvStoreState.LeaderState.CLOSING_WRITER, cursor);

        final int stateCtr = state.getStateCtr();
        logWriter.close()
                .whenComplete((Void v, Throwable t) -> {
                    if (state.isInState(stateCtr)) {
                        if (t != null) {
                            if (shouldAbdicate(t)) {
                                logger.logError("Replication has faltered at: " + cursor + " and will now abdicate");
                                abdicate();
                            } else {
                                logger.logError("Replication has faltered at: " + cursor + " and the close failed");
                                state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_1, cursor);
                            }
                        } else {
                            logger.logInfo("Replication has faltered at: " + cursor);
                            state.changeLeaderState(KvStoreState.LeaderState.START_SEQ_7_NEED_WRITER, cursor);
                            cursor.setEndOfLedger(true);
                            lastLedgerList = logWriter.getCachedLedgerList();              }
                    } else {
                        logger.logDebug("Ignoring stale completion: LogWriter::close");
                    }
                });
    }

    private boolean applyOp() {
        if (leaderIs(KvStoreState.LeaderState.READY) && opLog.hasUnappliedOps()) {
            Op op = opLog.getNextUnappliedOp();

            if (!isNoOp(op)) {
                ObjectNode replyBody = kvStore.apply(op.getFields());
                send(op.getFields().get(Fields.SOURCE),
                        replyBody.get(Fields.MSG_TYPE).asText(),
                        replyBody);
            }
            return true;
        } else if (isFollower() && opLog.hasUnappliedOps()) {
            Op op = opLog.getNextUnappliedOp();
            if (!isNoOp(op)) {
                kvStore.apply(op.getFields());
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean startReader() {
        if (followerIs(KvStoreState.FollowerState.NEED_READER)) {
            state.changeFollowerState(KvStoreState.FollowerState.READING, cursor);
            logReader = newLogReader();
            logReader.start();
            return true;
        } else {
            return false;
        }
    }

    private boolean keepReading() {
        if (followerIs(KvStoreState.FollowerState.READING)) {
            return logReader.read();
        } else {
            return false;
        }
    }

    private boolean shouldAppendNoOp() {
        if (!leaderIs(KvStoreState.LeaderState.READY)) {
            return false;
        }

        if (cursor.equals(lastPosOfNoOpCheck)) {
            return false;
        }

        // if the last op is a NoOp and it isn't committed yet, then don't append
        // another one
        Op lastOP = opLog.getLastOp();
        if (lastOP != null
                && lastOP.getFields().get(Fields.KV.Op.TYPE).equals(Constants.KvStore.Ops.NOOP)
                && lastOP.isCommitted() == false) {
            return false;
        }

        // else if its been longer than the check interval then yes
        return Duration.between(lastAppendedNoOp, Instant.now()).toMillis() > Constants.KvStore.MaxMsSinceLastOp;
    }

    private boolean appendNoOp() {
        if (shouldAppendNoOp()) {
            Map<String,String> noOpFields = new HashMap<>();
            noOpFields.put(Fields.KV.Op.TYPE, Constants.KvStore.Ops.NOOP);
            noOpFields.put(Fields.KV.Op.VALUE, nodeId);
            opLog.appendUncommitted(noOpFields, true);

            lastPosOfNoOpCheck = new Position(cursor);
            lastAppendedNoOp = Instant.now();
            return true;
        } else {
            return false;
        }
    }

    private boolean isLeader() {
        return state.role == KvStoreState.Role.LEADER;
    }

    private boolean leaderIs(KvStoreState.LeaderState leaderState) {
        return isLeader() && state.leaderState == leaderState;
    }

    private boolean isFollower() {
        return state.role == KvStoreState.Role.FOLLOWER;
    }

    private boolean followerIs(KvStoreState.FollowerState followerState) {
        return isFollower() && state.followerState == followerState;
    }

    private boolean isNoOp(Op op) {
        return op.getFields().get(Fields.KV.Op.TYPE).equals(Constants.KvStore.Ops.NOOP);
    }

    @Override
    void handleRequest(JsonNode request) {
        if (mayBeRedirect(request)) {
            return;
        }

        JsonNode body = request.get(Fields.BODY);
        String type = body.get(Fields.MSG_TYPE).asText();

        if (sessionManager.handlesRequest(type)) {
            sessionManager.handleRequest(request);
        } else if (type.equals(Commands.PRINT_STATE)) {
            printState();
        } else {
            Map<String, String> opData = new HashMap<>();
            opData.put(Fields.SOURCE, request.get(Fields.SOURCE).asText());
            opData.put(Fields.MSG_ID, body.get(Fields.MSG_ID).asText());
            opData.put(Fields.MSG_TYPE, body.get(Fields.MSG_TYPE).asText());
            opData.put(Fields.KV.Op.KEY, body.get(Fields.KV.Op.KEY).asText());

            boolean appendToMain = leaderIs(KvStoreState.LeaderState.READY);

//            if (!appendToMain) {
//                logger.logDebug("Rejected op - leader not ready. Leader is: " + state.leaderState
//                        + " Op: " + body.get(Fields.MSG_TYPE).asText());
//                failOp(opData, "Rejected");
//                return;
//            }

            switch (type) {
                case Constants.KvStore.Ops.READ:
                    logger.logDebug("Appended READ to OpLog");
                    opLog.appendUncommitted(opData, appendToMain);
                    break;
                case Constants.KvStore.Ops.WRITE:
                    logger.logDebug("Appended WRITE to OpLog");
                    opData.put(Fields.KV.Op.VALUE, body.get(Fields.KV.Op.VALUE).asText());
                    opLog.appendUncommitted(opData, appendToMain);
                    break;
                case Constants.KvStore.Ops.CAS:
                    logger.logDebug("Appended CAS to OpLog");
                    opData.put(Fields.KV.Op.FROM, body.get(Fields.KV.Op.FROM).asText());
                    opData.put(Fields.KV.Op.TO, body.get(Fields.KV.Op.TO).asText());
                    opLog.appendUncommitted(opData, appendToMain);
                    break;
                default:
                    logger.logError("Bad message type: " + type);
            }
        }
    }

    void printState() {
        logger.logInfo("----------------- KV Store Node -------------");
        logger.logInfo("Role: " + state.role + System.lineSeparator()
            + "Leader state: " + state.leaderState + System.lineSeparator()
            + "Follower state: " + state.followerState + System.lineSeparator()
            + "Cursor: " + cursor + System.lineSeparator()
            + "Cached leader. Version: " + cachedLeaderId.getVersion()
                        + " Node: " + cachedLeaderId.getValue() + System.lineSeparator());

        opLog.printState();
        kvStore.printState();

        if (logReader != null) {
            logReader.printState();
        }

        if (logWriter != null) {
            logWriter.printState();
        }

        logger.logInfo("---------------------------------------------");
    }

    private boolean mayBeRedirect(JsonNode request) {
        String type = request.get(Fields.BODY).get(Fields.MSG_TYPE).asText();
        if (Constants.KvStore.Ops.Types.contains(type)) {
            if (!isLeader()) {
                if (cachedLeaderId.getValue().equals(Constants.Metadata.NoLeader)
                    || cachedLeaderId.getValue().equals(nodeId)) {
                    // the cached leader may still be this node because we abdicated
                    // but don't yet know who the leader is
                    replyWithError(request, 11, "not a leader");
                } else {
                    proxy(request, cachedLeaderId.getValue());
                }
                return true;
            }
        }

        return false;
    }

    private LogWriter newLogWriter() {
        return new LogWriter(builder,
                this,
                (position, op) -> advancedCommittedIndex(position, op));
    }

    private LogReader newLogReader() {
        return new LogReader(builder,
                this,
                (position, op) -> appendOp(position, op),
                () -> cursor,
                false,
                -1L);
    }

    private LogReader newCatchupLogReader(long recoveredLedgerId) {
        return new LogReader(builder,
                this,
                (position, op) -> appendOp(position, op),
                () -> cursor,
                true,
                recoveredLedgerId);
    }

    private LogSegmentCloser newLogSegmentCloser() {
        return new LogSegmentCloser(builder,this);
    }

    // called by writers to advance the committed index
    private void advancedCommittedIndex(Position newPosition,
                                        Op op) {
        if (newPosition.getLedgerId() >= cursor.getLedgerId()
                && newPosition.getEntryId() > cursor.getEntryId()) {
            cursor = newPosition;
            opLog.committed(op);
            logger.logDebug("committed: " + newPosition.toString() +
                    "\t" + Op.opToString(op));
        } else if (newPosition.getLedgerId() >= cursor.getLedgerId()
            && newPosition.getEntryId() == -1L) {
            // opened a new ledger
            cursor = newPosition;
            logger.logDebug("advance cursor to new ledger: " + newPosition.toString());
        }
        else {
            logger.logInfo("Tried to update the cursor with a lower position");
        }
    }

    // called by readers to append committed ops to the op log
    private void appendOp(Position newPosition,
                          Op op) {
        if (newPosition.getLedgerId() >= cursor.getLedgerId()
                && newPosition.getEntryId() > cursor.getEntryId()) {
            cursor = newPosition;
            op.setCommitted(true);
            opLog.appendCommitted(op);
        } else if (newPosition.getLedgerId() >= cursor.getLedgerId()
                && newPosition.getEntryId() == -1L) {
            // opened a new ledger
            cursor = newPosition;
        }
        else {
            logger.logInfo("Tried to update the cursor with a lower position");
        }
    }

    private boolean shouldAbdicate(Throwable t) {
        t = Futures.unwrap(t);
        if (t instanceof BkException) {
            BkException bke = (BkException) t;
            if (bke.getCode().equals(ReturnCodes.Metadata.BAD_VERSION)
                || bke.getCode().equals(ReturnCodes.Bookie.FENCED)) {
                return true;
            }
        } else if (t instanceof MetadataException) {
            MetadataException me = (MetadataException) t;
            if (me.getCode().equals(ReturnCodes.Metadata.BAD_VERSION)) {
                return true;
            }
        }

        return false;
    }

    private void abdicate() {
        try {
            cancelAllOperations();
            state.changeRole(KvStoreState.Role.NONE);
            state.changeLeaderState(KvStoreState.LeaderState.NONE, cursor);
            truncateUncommittedOps("Not leader");
            checkLeadership();
        } catch (Throwable t) {
            logger.logError("Failed during abdication. Should not happen.", t);
        }
    }

    private void cancelAllOperations() {
        if (logWriter != null) {
            logWriter.cancel();
            logWriter = null;
        }

        if (logReader != null) {
            logReader.cancel();
            logReader = null;
        }

        if (logSegmentCloser != null) {
            logSegmentCloser.cancel();
            logSegmentCloser = null;
        }
    }
}
