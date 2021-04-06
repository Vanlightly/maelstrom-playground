package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.kv.*;
import com.vanlightly.bookkeeper.kv.bkclient.BkException;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;
import com.vanlightly.bookkeeper.kv.log.*;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.network.NetworkIO;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class KvStoreNode extends Node {
    MetadataManager metadataManager;
    LedgerManager ledgerManager;
    LogWriter logWriter;
    LogReader logReader;
    KvStore kvStore;
    OpLog opLog;

    KvStoreState state;
    Position cursor;
    Versioned<String> cachedLeaderId;

    private Instant lastUpdatedMetadata;
    private boolean pendingLeaderResult;

    public KvStoreNode(String nodeId,
                       NetworkIO net,
                       Logger logger,
                       ObjectMapper mapper,
                       ManagerBuilder builder) {
        super(nodeId, false, net, logger, mapper, builder);
        this.metadataManager = builder.buildMetadataManager(sessionManager, this);
        this.ledgerManager = builder.buildLedgerManager(sessionManager, this);
        this.lastUpdatedMetadata = Instant.now().minus(1, ChronoUnit.DAYS);
        this.kvStore = new KvStore(mapper);
        this.opLog = new OpLog();
        this.state = new KvStoreState(logger);
        this.cursor = new Position(-1L, -1L);
        this.cachedLeaderId = new Versioned<>(Constants.Metadata.NoLeader, -1);
    }


    @Override
    public void initialize(JsonNode initMsg) {
        sendInitOk(initMsg);
    }

    @Override
    public boolean roleSpecificAction() {
        return sessionManager.maintainSession()
                || checkLeadership()
                || closeCurrentLogSegment()
                || startCatchUpReader()
                || keepCatchingUp()
                || startWriter()
                || replicate()
                || applyOp()
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
                    .whenComplete((Versioned<String> vLeaderId, Throwable t) -> {
                        pendingLeaderResult = false;
                        if (t != null) {
                            logger.logError("Failed to obtain latest leader id", t);
                        } else {
                            reactToLeaderUpdate(vLeaderId);
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

                    // cancel operations
                    if (logWriter != null) {
                        logWriter.cancel();
                        logWriter = null;
                    } else if (logReader != null) {
                        logReader.cancel();
                        logReader = null;
                    }

                    if (nodeId.equals(vLeaderId.getValue())) {
                        state.changeRole(KvStoreState.Role.LEADER);
                        state.changeLeaderState(KvStoreState.LeaderState.NEED_CLOSE_SEGMENT);
                    } else {
                        // abdicate leadership
                        state.changeRole(KvStoreState.Role.FOLLOWER);
                        state.changeFollowerState(KvStoreState.FollowerState.NEED_READER);
                    }
                }
                break;
            case FOLLOWER:
                if (cachedLeaderId.getVersion() < vLeaderId.getVersion()) {
                    if (nodeId.equals(vLeaderId.getValue())) {
                        // this node is the new leader, so cancel the reader
                        logReader.cancel();
                        logReader = null;

                        state.changeRole(KvStoreState.Role.LEADER);
                        state.changeLeaderState(KvStoreState.LeaderState.NEED_CLOSE_SEGMENT);
                    }
                }
                break;
            default:
                // first leader result
                cachedLeaderId = vLeaderId;
                if (cachedLeaderId.getValue().equals(nodeId)) {
                    state.changeRole(KvStoreState.Role.LEADER);
                    state.changeLeaderState(KvStoreState.LeaderState.NEED_CATCHUP_READER);
                } else {
                    state.changeRole(KvStoreState.Role.FOLLOWER);
                    state.changeFollowerState(KvStoreState.FollowerState.NEED_READER);
                }
                break;
        }

        cachedLeaderId = vLeaderId;
    }

    private boolean closeCurrentLogSegment() {
        if (leaderIs(KvStoreState.LeaderState.NEED_CLOSE_SEGMENT)) {
            final int stateCtr = state.changeLeaderState(KvStoreState.LeaderState.CLOSING_SEGMENT);
            LogSegmentCloser closer = newLogSegmentCloser();
            closer.closeSegment(cursor)
                    .whenComplete((Void v, Throwable t) -> {
                        if (state.isInState(stateCtr)) {
                            if (t != null) {
                                logger.logError("Failed closing the current log segment. Will try again.", t);
                                state.changeLeaderState(KvStoreState.LeaderState.NEED_CLOSE_SEGMENT);
                            } else {
                                logger.logInfo("Log segment closed.");
                                state.changeLeaderState(KvStoreState.LeaderState.NEED_CATCHUP_READER);
                            }
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
        if (leaderIs(KvStoreState.LeaderState.NEED_CATCHUP_READER)) {
            state.changeLeaderState(KvStoreState.LeaderState.CATCHUP_READING);
            logReader = newCatchupLogReader();
            logReader.start(cursor);
            return true;
        } else {
            return false;
        }
    }

    private boolean keepCatchingUp() {
        if (leaderIs(KvStoreState.LeaderState.CATCHUP_READING)
                && logReader.hasCaughtUp() == false) {
            return logReader.read();
        } else {
            return false;
        }
    }

    private boolean startWriter() {
        if ((leaderIs(KvStoreState.LeaderState.CATCHUP_READING) && logReader.hasCaughtUp())
                || leaderIs(KvStoreState.LeaderState.NEED_WRITER)) {
            final int stateCtr = state.changeLeaderState(KvStoreState.LeaderState.STARTING_WRITER);

            logWriter = newLogWriter();
            logWriter.start()
                    .whenComplete((Void v, Throwable t) -> {
                        if (state.isInState(stateCtr)) {
                            if (t != null) {
                                logger.logError("Writer failed to start", t);
                                logWriter.cancel();
                                logWriter = null;
                                state.changeLeaderState(KvStoreState.LeaderState.NEED_WRITER);
                            } else {
                                state.changeLeaderState(KvStoreState.LeaderState.READY);
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
            final int stateCtr = state.getStateCtr();
            logWriter.write(value)
                    .whenComplete((Void v, Throwable t) -> {
                        if (t != null) {
                            // a write can only fail when there are no enough non-faulty bookies
                            if (state.isInState(stateCtr)) {
                                state.changeLeaderState(KvStoreState.LeaderState.CLOSING_WRITER);

                                String text = t.getMessage();
                                if (t instanceof BkException) {
                                    BkException bke = (BkException) t;
                                    text = bke.getCode();
                                }

                                logger.logInfo("Replication failure, nacking all pending ops");

                                // nack all pending operations
                                List<Op> uncommitedOps = opLog.clearUncomittedOps();

                                for (Op failedOp : uncommitedOps) {
                                    ObjectNode reply = mapper.createObjectNode();
                                    reply.put(Fields.IN_REPLY_TO, failedOp.getFields().get(Fields.MSG_ID));
                                    reply.put("error", 1);
                                    reply.put("text", text);
                                    send(op.getFields().get(Fields.SOURCE), op.getFields().get(Fields.MSG_TYPE), reply);
                                }

                                final int stateCtr2 = state.getStateCtr();
                                logWriter.close()
                                        .whenComplete((Void v2, Throwable t2) -> {
                                            if (state.isInState(stateCtr2)) {
                                                if (t != null) {
                                                    logger.logError("Replication has faltered at: " + cursor + " and the close failed");
                                                    state.changeLeaderState(KvStoreState.LeaderState.NEED_CLOSE_SEGMENT);
                                                } else {
                                                    logger.logInfo("Replication has faltered at: " + cursor);
                                                    state.changeLeaderState(KvStoreState.LeaderState.NEED_WRITER);
                                                    cursor.setEndOfLedger(true);
                                                }
                                            } else {
                                                logger.logDebug("Ignoring stale completion: LogWriter::close");
                                            }
                                        });
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

    public boolean applyOp() {
        if (leaderIs(KvStoreState.LeaderState.READY) && opLog.hasUnappliedOps()) {
            Op op = opLog.getNextUnappliedOp();
            ObjectNode replyBody = kvStore.apply(op.getFields());
            send(op.getFields().get(Fields.SOURCE),
                    op.getFields().get(Fields.MSG_TYPE),
                    replyBody);
            return true;
        } else {
            return false;
        }
    }

    private boolean startReader() {
        if (followerIs(KvStoreState.FollowerState.NEED_READER)) {
            state.changeFollowerState(KvStoreState.FollowerState.READING);
            logReader = newLogReader();
            logReader.start(cursor);
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

    @Override
    void handleRequest(JsonNode request) {
//        logger.logDebug("Received request: " + request.toString());
        JsonNode body = request.get(Fields.BODY);
        String type = body.get(Fields.MSG_TYPE).asText();

        Map<String, String> fields = new HashMap<>();
        fields.put(Fields.SOURCE, body.get(Fields.SOURCE).asText());
        fields.put(Fields.MSG_TYPE, body.get(Fields.MSG_TYPE).asText());
        fields.put(Fields.KV.KEY, body.get(Fields.KV.KEY).asText());
        Op op = new Op(fields);

        switch (type) {
            case Constants.KvStore.Ops.READ:
                opLog.add(op);
                break;
            case Constants.KvStore.Ops.WRITE:
                fields.put(Fields.KV.VALUE, body.get(Fields.KV.VALUE).asText());
                opLog.add(op);
                break;
            case Constants.KvStore.Ops.CAS:
                fields.put(Fields.KV.FROM, body.get(Fields.KV.FROM).asText());
                fields.put(Fields.KV.TO, body.get(Fields.KV.TO).asText());
                opLog.add(op);
                break;
            default:
                logger.logError("Bad message type: " + type);
        }
    }

    private LogWriter newLogWriter() {
        return new LogWriter(metadataManager,
                ledgerManager,
                mapper,
                logger,
                this,
                (position, op) -> advancedCommittedIndex(position, op));
    }

    private LogReader newLogReader() {
        return new LogReader(metadataManager,
                ledgerManager,
                mapper,
                logger,
                this,
                (position, op) -> advancedCommittedIndex(position, op),
                false);
    }

    private LogReader newCatchupLogReader() {
        return new LogReader(metadataManager,
                ledgerManager,
                mapper,
                logger,
                this,
                (position, op) -> advancedCommittedIndex(position, op),
                true);
    }

    private LogSegmentCloser newLogSegmentCloser() {
        return new LogSegmentCloser(metadataManager,
                ledgerManager,
                mapper,
                logger,
                this);
    }

    private void advancedCommittedIndex(Position newPosition,
                                        Op op) {
        if (newPosition.getLedgerId() >= cursor.getLedgerId()
                && newPosition.getEntryId() >= cursor.getEntryId()) {
            cursor = newPosition;
            opLog.committed(op);
        } else {
            logger.logInfo("Tried to update the cursor with a lower position");
        }
    }
}
