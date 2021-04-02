package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.Session;
import com.vanlightly.bookkeeper.metadata.Versioned;
import com.vanlightly.bookkeeper.network.NetworkIO;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetadataStoreNode extends Node {
    Map<String, Session> sessions;

    // kvstore metadata
    Versioned<List<Long>> ledgerList;
    Versioned<String> leader;

    // bookkeeper metadata
    Set<String> availableBookies;
    Set<String> availableClients;
    Map<Long, Versioned<LedgerMetadata>> ledgers;

    Instant lastCheckedSessions;
    final Long expiryNs;
    final Long expiryCheckMs;
    ScheduledExecutorService expiryScheduler;

    long nextSessionId;
    long nextLedgerId;

    public MetadataStoreNode(String nodeId,
                             NetworkIO net,
                             Logger logger,
                             ObjectMapper mapper,
                             ManagerBuilder builder) {
        super(nodeId, false, net, logger, mapper, builder);
        this.sessions = new HashMap<>();
        this.ledgerList = new Versioned<>(new ArrayList<>(), 0);
        this.availableBookies = new HashSet<>();
        this.availableClients = new HashSet<>();
        this.ledgers = new HashMap<>();
        this.leader = new Versioned<>(Constants.Metadata.NoLeader, 0);

        this.lastCheckedSessions = Instant.now();
        this.expiryNs = Constants.KeepAlives.KeepAliveExpiryMs * 1000000L;
        this.expiryCheckMs = Constants.KeepAlives.KeepAliveCheckMs;

        this.expiryScheduler = Executors.newSingleThreadScheduledExecutor();
        this.expiryScheduler.scheduleWithFixedDelay(() -> {
            checkSessions();
        }, this.expiryCheckMs, this.expiryCheckMs, TimeUnit.MILLISECONDS);

        this.nextLedgerId = -1L;
        this.nextSessionId = 0L;
    }

    @Override
    void initialize(JsonNode initMsg) {
        sendInitOk(initMsg);
    }

    @Override
    boolean roleSpecificAction() {
        return checkSessions();
    }

    @Override
    void handleRequest(JsonNode request) {
        logger.logDebug(request.toString());
        try {
            String type = request.get(Fields.BODY).get(Fields.MSG_TYPE).asText();
            switch (type) {
                case Commands.Metadata.SESSION_NEW:
                    handleNewSession(request);
                    break;
                case Commands.Metadata.SESSION_KEEP_ALIVE:
                    verifySession(request);
                    break;
                case Commands.Metadata.GET_LEADER_ID:
                    handleGetLeaderId(request);
                    break;
                case Commands.Metadata.GET_LEDGER_LIST:
                    handleGetLedgerList(request);
                    break;
                case Commands.Metadata.BK_METADATA_READ:
                    handleReadBkMetadata(request);
                    break;
                case Commands.Metadata.LEDGER_READ:
                    handleReadLedger(request);
                    break;
                case Commands.Metadata.LEDGER_UPDATE:
                    handleUpdateLedger(request);
                    break;
                case Commands.Metadata.LEDGER_CREATE:
                    handleCreateLedger(request);
                    break;
                case Commands.Metadata.LEDGER_LIST_UPDATE:
                    handleLedgerListUpdate(request);
                    break;
                default:
                    logger.logError("Bad message type: " + type);
            }
            checkSessions();
        } catch (JsonProcessingException e) {
            logger.logError("Failed parsing JSON message: " + request.toString(), e);
        }
    }

    private boolean shouldCheckSessions() {
        return Duration.between(lastCheckedSessions, Instant.now()).toMillis() > expiryCheckMs;
    }

    private boolean checkSessions() {
        if (shouldCheckSessions()) {
            try {
                List<String> expiredNodeIds = removeExpiredSessions();
                removeExpired(availableBookies);
                removeExpired(availableClients);

                mayBeChangeLeader();

                for (String nodeId : expiredNodeIds) {
                    send(nodeId, Commands.Metadata.SESSION_EXPIRED);
                }

                lastCheckedSessions = Instant.now();
            } catch (Exception e) {
                logger.logError("Failed checking for expired sessions", e);
            }
            return true;
        } else {
            return false;
        }
    }

    private void mayBeChangeLeader() {
        boolean leaderIsSet = !leader.equals(Constants.Metadata.NoLeader);
        boolean leaderAvailable = availableClients.contains(leader);

        /* A leader change is required if either:
            - there is no current leader
            - the current leader is unavailable
         */
        boolean leaderChangeRequired = !leaderIsSet || (leaderIsSet && !leaderAvailable);

        if (leaderChangeRequired) {
            if (availableClients.isEmpty()) {
                // there is no leader and no available clients to become leader
            } else {
                // the current leader is not available, so choose a new one randomly
                leader.setValue(availableClients.stream().findFirst().get());
                leader.incrementVersion();
            }
        }
    }

    private List<String> removeExpiredSessions() {
        long now = System.nanoTime();
        List<String> removed = new ArrayList<>();

        for(String nodeId : sessions.keySet()) {
            if (sessions.get(nodeId).getRenewDeadline() < now) {
                sessions.remove(nodeId);
                removed.add(nodeId);
            }
        }

        return removed;
    }

    private void removeExpired(Set<String> nodeSet) {
        for(String nodeId : nodeSet) {
            if (!sessions.containsKey(nodeId)) {
                nodeSet.remove(nodeId);
            }
        }
    }

    private void addBookKeeperMetadata(ObjectNode body) {
        body.set("available_bookies", toJsonArray(availableBookies));
        body.set("available_clients", toJsonArray(availableClients));
    }

    private ArrayNode toJsonArray(Set<String> list) {
        ArrayNode jsonArray = mapper.createArrayNode();

        for (String item : list) {
            jsonArray.add(item);
        }

        return jsonArray;
    }

    private Session getSession(JsonNode msg) {
        JsonNode body = msg.get(Fields.BODY);
        long nodeSessionId = body.get(Fields.SESSION_ID).asLong();
        String nodeId = msg.get(Fields.SOURCE).asText();

        Session session = sessions.get(nodeId);
        if (session != null && session.getSessionId() == nodeSessionId) {
            return session;
        } else {
            return null;
        }
    }

    private void replyBadSession(JsonNode msg) {
        reply(msg, ReturnCodes.Metadata.BAD_SESSION);
    }

    private void replyBadVersion(JsonNode msg) {
        reply(msg, ReturnCodes.Metadata.BAD_VERSION);
    }

    private void handleNewSession(JsonNode msg) {
        String nodeId = msg.get(Fields.SOURCE).asText();

        if (Node.determineType(nodeId) == NodeType.Bookie) {
            availableBookies.add(nodeId);
        } else if (Node.determineType(nodeId) == NodeType.KvStore) {
            availableClients.add(nodeId);
        }

        nextSessionId++;
        Session session = new Session(nodeId,
                System.nanoTime() + expiryNs,
                nextSessionId);
        sessions.put(nodeId, session);

        ObjectNode replyBody = mapper.createObjectNode();
        replyBody.put(Fields.SESSION_ID, nextSessionId);
        reply(msg, ReturnCodes.OK, replyBody);
    }

    private void handleGetLeaderId(JsonNode msg) {
        Session session = verifySession(msg);
        if (session.isValid()) {
            ObjectNode replyBody = mapper.createObjectNode();
            replyBody.put(Fields.SESSION_ID, session.getSessionId());
            replyBody.put(Fields.KV.LEADER, leader.getValue());
            replyBody.put(Fields.KV.LEADER_VERSION, leader.getVersion());
            reply(msg, ReturnCodes.OK, replyBody);
        }
    }

    private void handleGetLedgerList(JsonNode msg) {
        Session session = verifySession(msg);
        if (session.isValid()) {
            ObjectNode replyBody = mapper.createObjectNode();
            replyBody.put(Fields.SESSION_ID, session.getSessionId());
            replyBody.put(Fields.KV.LEDGER_LIST_VERSION, ledgerList.getVersion());
            replyBody.set(Fields.KV.LEDGER_LIST, mapper.valueToTree(ledgerList.getValue()));
            reply(msg, ReturnCodes.OK, replyBody);
        }
    }

    private void handleReadBkMetadata(JsonNode msg) {
        Session session = verifySession(msg);
        if (session.isValid()) {
            ObjectNode replyBody = mapper.createObjectNode();
            replyBody.put(Fields.SESSION_ID, session.getSessionId());
            addBookKeeperMetadata(replyBody);
            reply(msg, ReturnCodes.OK, replyBody);
        }
    }

    private void handleReadLedger(JsonNode msg) throws JsonProcessingException {
        Session session = verifySession(msg);
        if (session.isValid()) {
            JsonNode body = msg.get(Fields.BODY);
            long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();

            Versioned<LedgerMetadata> current = ledgers.get(ledgerId);
            if (current == null) {
                reply(msg, ReturnCodes.Metadata.NO_SUCH_LEDGER);
                return;
            }

            ObjectNode replyBody = mapper.createObjectNode();
            replyBody.put(Fields.SESSION_ID, session.getSessionId());
            replyBody.put(Fields.L.LEDGER_ID, current.getValue().getLedgerId());
            replyBody.set(Fields.M.LEDGER_METADATA, mapper.readTree(mapper.writeValueAsString(current.getValue())));
            reply(msg, ReturnCodes.OK, replyBody);
        }
    }

    private void handleUpdateLedger(JsonNode msg) throws JsonProcessingException {
        Session session = verifySession(msg);
        if (session.isValid()) {
            JsonNode body = msg.get(Fields.BODY);
            long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();

            Versioned<LedgerMetadata> current = ledgers.get(ledgerId);
            if (current == null) {
                reply(msg, ReturnCodes.Metadata.NO_SUCH_LEDGER);
                return;
            }

            long version = body.get(Fields.VERSION).asInt();
            LedgerMetadata md = mapper.treeToValue(body.get("metadata"), LedgerMetadata.class);
            Versioned<LedgerMetadata> updated = new Versioned<>(md, version);

            if (updated.getVersion() != current.getVersion()) {
                reply(msg, ReturnCodes.Metadata.BAD_VERSION);
                return;
            }

            updated.incrementVersion();
            ledgers.put(ledgerId, updated);

            ObjectNode replyBody = mapper.createObjectNode();
            replyBody.put(Fields.SESSION_ID, session.getSessionId());
            replyBody.put(Fields.L.LEDGER_ID, updated.getValue().getLedgerId());
            replyBody.set(Fields.M.LEDGER_METADATA, mapper.readTree(mapper.writeValueAsString(updated.getValue())));
            reply(msg, ReturnCodes.OK, replyBody);
        }
    }

    private void handleCreateLedger(JsonNode msg) throws JsonProcessingException {
        Session session = verifySession(msg);
        if (session.isValid()) {
            JsonNode body = msg.get(Fields.BODY);
            LedgerMetadata md = mapper.treeToValue(body.get(Fields.M.LEDGER_METADATA), LedgerMetadata.class);
            Versioned<LedgerMetadata> newLedger = new Versioned<>(md, 0);

            nextLedgerId++;
            md.setLedgerId(nextLedgerId);

            ledgers.put(nextLedgerId, newLedger);

            ObjectNode res = mapper.createObjectNode();
            res.put(Fields.L.LEDGER_ID, nextLedgerId);
            res.put(Fields.VERSION, 0);
            res.set(Fields.M.LEDGER_METADATA, mapper.valueToTree(md));
            reply(msg, ReturnCodes.OK, res);
        }
    }

    private void handleLedgerListUpdate(JsonNode msg) {
        Session session = verifySession(msg);
        if (session.isValid()) {
            JsonNode body = msg.get(Fields.BODY);
            long version = body.get(Fields.KV.LEDGER_LIST_VERSION).asLong();
            List<Long> updatedLedgerList = new ArrayList<>();
            ArrayNode llNode = (ArrayNode) body.get(Fields.KV.LEDGER_LIST);
            for (JsonNode ledgerId : llNode) {
                updatedLedgerList.add(ledgerId.asLong());
            }

            if (ledgerList.getVersion() == version) {
                ledgerList.incrementVersion();
                ledgerList.setValue(updatedLedgerList);

                ObjectNode replyBody = mapper.createObjectNode();
                replyBody.put(Fields.KV.LEDGER_LIST_VERSION, ledgerList.getVersion());
                replyBody.set(Fields.KV.LEDGER_LIST, mapper.valueToTree(ledgerList.getValue()));
                reply(msg, ReturnCodes.OK, replyBody);
            } else {
                replyBadVersion(msg);
            }
        }
    }

    private Session verifySession(JsonNode msg) {
        Session session = getSession(msg);
        if (session == null) {
            replyBadSession(msg);
            return Session.invalidSession();
        } else {
            renewSession(session);
            return session;
        }
    }

    private void renewSession(Session session) {
        long renewDeadline = System.nanoTime() + expiryNs;
        session.setRenewDeadline(renewDeadline);
    }
}
