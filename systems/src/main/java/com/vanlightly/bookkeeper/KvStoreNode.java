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
    Versioned<List<Long>> cachedLedgerList;
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
        this.state = new KvStoreState();
        this.cursor = new Position(-1L, -1L);
    }


    @Override
    public void initialize(JsonNode initMsg) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        FutureRetries.retryForever(future, () -> doFirstInitialize(initMsg));
    }

    private CompletableFuture<Void> doFirstInitialize(JsonNode initMsg) {
        return doInitialize()
                .thenRun(() -> {
                    sendInitOk(initMsg);
                });
    }

    private CompletableFuture<Void> doInitialize() {
        return metadataManager.getLeader()
                .thenAccept((Versioned<String> vLeaderId) -> {
                    cachedLeaderId = vLeaderId;
                    if (cachedLeaderId.getValue().equals(nodeId)) {
                        state.role = KvStoreState.Role.LEADER;
                        state.leaderState = KvStoreState.LeaderState.NEED_CATCHUP_READER;
                    } else {
                        state.role = KvStoreState.Role.FOLLOWER;
                        state.followerState = KvStoreState.FollowerState.NEED_READER;
                    }
                });
    }

    @Override
    public boolean roleSpecificAction() {
        return sessionManager.maintainSession()
                || checkLeadership()
                || startCatchUpReader()
                || keepCatchingUp()
                || startWriter()
                || replicate()
                || applyOp()
                || startReader()
                || keepReading();
    }

    private boolean shouldCheckLeadership() {
        boolean isReady = ((state.role == KvStoreState.Role.LEADER && state.leaderState == KvStoreState.LeaderState.READY)
                || (state.role == KvStoreState.Role.FOLLOWER && state.followerState == KvStoreState.FollowerState.READING));

        return Duration.between(lastUpdatedMetadata, Instant.now()).toMillis() > Constants.KvStore.CheckLeadershipIntervalMs
                && !pendingLeaderResult
                && isReady;
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
                        state.role = KvStoreState.Role.LEADER;
                        state.leaderState = KvStoreState.LeaderState.NEED_CLOSE_SEGMENT;
                    } else {
                        // abdicate leadership
                        state.role = KvStoreState.Role.FOLLOWER;
                        state.followerState = KvStoreState.FollowerState.NEED_READER;
                    }
                }
                break;
            case FOLLOWER:
                if (cachedLeaderId.getVersion() < vLeaderId.getVersion()) {
                    if (nodeId.equals(vLeaderId.getValue())) {
                        // this node is the new leader, so cancel the reader
                        logReader.cancel();
                        logReader = null;

                        state.role = KvStoreState.Role.LEADER;
                        state.leaderState = KvStoreState.LeaderState.NEED_CLOSE_SEGMENT;
                    }
                }
                break;
            default:
                break;
        }

        cachedLeaderId = vLeaderId;
    }

    private boolean closeCurrentSegment() {
        if (leaderIs(KvStoreState.LeaderState.NEED_CLOSE_SEGMENT)) {


            return true;
        } else {
            return false;
        }
    }

    private boolean startCatchUpReader() {
        if (leaderIs(KvStoreState.LeaderState.NEED_CATCHUP_READER)) {
            state.leaderState = KvStoreState.LeaderState.STARTING_CATCHUP_READER;
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
            switch (logReader.getReaderState()) {
                case NO_LEDGER:
                    logReader.openNextLedger();
                    return true;
                case IDLE:
                    logReader.readUpToLac();
                    return true;
                default:
                    return logReader.updateCachedLedgerMetadata();
            }
        } else {
            return false;
        }
    }

    private boolean startWriter() {
        if ((leaderIs(KvStoreState.LeaderState.CATCHUP_READING) && logReader.hasCaughtUp())
                && leaderIs(KvStoreState.LeaderState.NEED_WRITER)) {
            state.leaderState = KvStoreState.LeaderState.STARTING_WRITER;

            logWriter = newLogWriter();
            logWriter.start()
                    .whenComplete((Void v, Throwable t) -> {
                        if (leaderIs(KvStoreState.LeaderState.STARTING_WRITER)) {
                            if (t != null) {
                                logger.logError("Writer failed to start", t);
                                logWriter.cancel();
                                logWriter = null;
                                state.leaderState = KvStoreState.LeaderState.NEED_WRITER;
                            } else {
                                logger.logDebug("New writer started, can start processing requests");
                                state.leaderState = KvStoreState.LeaderState.READY;
                            }
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
            logWriter.write(value)
                    .whenComplete((Void v, Throwable t) -> {
                        if (t != null) {
                            // a write can only fail when there are no enough non-faulty bookies
                            if (leaderIs(KvStoreState.LeaderState.READY)) {
                                state.leaderState = KvStoreState.LeaderState.CLOSING_WRITER;

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

                                logWriter.close()
                                        .whenComplete((Void v2, Throwable t2) -> {
                                            if (leaderIs(KvStoreState.LeaderState.CLOSING_WRITER)) {
                                                if (t != null) {
                                                    logger.logError("Replication has faltered at: " + cursor + " and the close failed");
                                                    state.leaderState = KvStoreState.LeaderState.NEED_CLOSE_SEGMENT;
                                                } else {
                                                    logger.logInfo("Replication has faltered at: " + cursor);
                                                    state.leaderState = KvStoreState.LeaderState.NEED_WRITER;
                                                    cursor.setEndOfLedger(true);
                                                }
                                            }
                                        });
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
            state.followerState = KvStoreState.FollowerState.READING;
            logReader = newLogReader();
            logReader.start(cursor);
            return true;
        } else {
            return false;
        }
    }

    private boolean keepReading() {
        if (followerIs(KvStoreState.FollowerState.READING)) {
            switch (logReader.getReaderState()) {
                case NO_LEDGER:
                    logReader.openNextLedger();
                    return true;
                case IDLE:
                    logReader.readUpToLac();
                    return true;
                default:
                    return logReader.updateCachedLedgerMetadata();
            }
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
