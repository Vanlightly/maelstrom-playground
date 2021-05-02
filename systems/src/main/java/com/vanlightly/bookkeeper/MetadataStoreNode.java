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
import com.vanlightly.bookkeeper.util.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

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

    long nextSessionId;
    long nextLedgerId;

    public MetadataStoreNode(String nodeId,
                             NetworkIO net,
                             ManagerBuilder builder) {
        super(nodeId, false, net, builder);
        this.sessions = new HashMap<>();
        this.ledgerList = new Versioned<>(new ArrayList<>(), 0);
        this.availableBookies = new HashSet<>();
        this.availableClients = new HashSet<>();
        this.ledgers = new HashMap<>();
        this.leader = new Versioned<>(Constants.Metadata.NoLeader, 0);

        this.lastCheckedSessions = Instant.now();
        this.expiryNs = Constants.KeepAlives.KeepAliveExpiryMs * 1000000L;
        this.expiryCheckMs = Constants.KeepAlives.KeepAliveCheckMs;

        this.nextLedgerId = -1L;
        this.nextSessionId = 0L;
    }

    @Override
    void initialize(JsonNode initMsg) {
        sendInitOk(initMsg);
    }

    @Override
    boolean roleSpecificAction() {
        return mayBeCheckSessions();
    }

    @Override
    void handleRequest(JsonNode request) {
//        logger.logDebug("Received request: " + request.toString());
        if (mayBeRedirect(request)) {
            return;
        }

        try {
            String type = request.get(Fields.BODY).get(Fields.MSG_TYPE).asText();
            switch (type) {
                case Commands.PRINT_STATE:
                    printState();
                    break;
                case Commands.Metadata.SESSION_NEW:
                    handleNewSession(request);
                    break;
                case Commands.Metadata.SESSION_KEEP_ALIVE:
                    handleKeepAlive(request);
                    break;
                case Commands.Metadata.GET_LEADER_ID:
                    handleGetLeaderId(request);
                    break;
                case Commands.Metadata.GET_LEDGER_ID:
                    handleGetLedgerId(request);
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

    void printState() {
        logger.logInfo("----------- Metadata Store State -------------");
        logger.logInfo("Sessions: ");
        for (Session session : sessions.values()) {
            logger.logInfo("\t-" + session.toString());
        }

        logger.logInfo("Leader. Version: " + leader.getVersion() + " Node: " + leader.getValue());
        logger.logInfo("Available bookies: " + availableBookies);
        logger.logInfo("Available clients: " + availableClients);
        logger.logInfo("LedgerList. Version: " + ledgerList.getVersion() + " List: " + ledgerList.getValue());
        logger.logInfo("Ledger metadata: ");
        for (Versioned<LedgerMetadata> md : ledgers.values()) {
            logger.logInfo("\t- Version: " + md.getVersion() + " Ledger: " + md.getValue().toString());
        }
        logger.logInfo("----------------------------------------------");
    }

    private boolean shouldCheckSessions() {
        return Duration.between(lastCheckedSessions, Instant.now()).toMillis() > expiryCheckMs;
    }

    private boolean mayBeCheckSessions() {
        if (shouldCheckSessions()) {
            checkSessions();
            return true;
        } else {
            return false;
        }
    }

    private void checkSessions() {
        try {
            List<Session> expiredSessions = removeExpiredSessions();
            removeExpired(availableBookies);
            removeExpired(availableClients);

            mayBeChangeLeader();

            for (Session session : expiredSessions) {
                logger.logDebug(session.getNodeId() + " session has expired!");
                ObjectNode body = mapper.createObjectNode();
                body.put(Fields.SESSION_ID, session.getSessionId());
                send(session.getNodeId(), Commands.Metadata.SESSION_EXPIRED, body);
            }
        } catch (Exception e) {
            logger.logError("Failed checking for expired sessions", e);
        } finally {
            lastCheckedSessions = Instant.now();
        }
    }

    private void mayBeChangeLeader() {
        boolean leaderIsSet = !leader.equals(Constants.Metadata.NoLeader);
        boolean leaderAvailable = availableClients.contains(leader.getValue());

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
                String oldLeader = leader.getValue();
                long oldLeaderVersion = leader.getVersion();
                String newLeader = availableClients.stream().findFirst().get();
                leader.setValue(newLeader);
                leader.incrementVersion();
                long newLeaderVersion = leader.getVersion();
                logger.logInfo("Leader change. From: " + oldLeader + ":" + oldLeaderVersion
                        + " to: " + newLeader + ":" + newLeaderVersion);
            }
        }
    }

    private List<Session> removeExpiredSessions() {
        long now = System.nanoTime();
        List<Session> removed = new ArrayList<>();

        Set<String> nodes = new HashSet<>(sessions.keySet());
        for(String nodeId : nodes) {
            if (sessions.get(nodeId).getRenewDeadline() < now) {
                removed.add(sessions.get(nodeId));
                sessions.remove(nodeId);
            }
        }

        return removed;
    }

    private void removeExpired(Set<String> nodeSet) {
        Set<String> nodeSetCopy = new HashSet<>(nodeSet);
        for(String nodeId : nodeSetCopy) {
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
        ObjectNode body = mapper.createObjectNode();
        body.put(Fields.SESSION_ID, msg.get(Fields.BODY).get(Fields.SESSION_ID).asText());
        reply(msg, ReturnCodes.Metadata.BAD_SESSION, body);
    }

    private void replyKeepAliveOk(JsonNode msg) {
        ObjectNode body = mapper.createObjectNode();
        body.put(Fields.SESSION_ID, msg.get(Fields.BODY).get(Fields.SESSION_ID).asText());
        reply(msg, ReturnCodes.OK, body);
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

        logger.logDebug("New session established. Node: " + nodeId + " Session: " + nextSessionId);

        ObjectNode replyBody = mapper.createObjectNode();
        replyBody.put(Fields.SESSION_ID, nextSessionId);
        reply(msg, ReturnCodes.OK, replyBody);
    }

    private void handleKeepAlive(JsonNode msg) {
        Session session = getSession(msg);

        if (session == null) {
            replyBadSession(msg);
        } else {
            renewSession(session);
            replyKeepAliveOk(msg);
        }
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
            replyBody.put(Fields.VERSION, current.getVersion());
            replyBody.set(Fields.M.LEDGER_METADATA, mapper.readTree(mapper.writeValueAsString(current.getValue())));
            reply(msg, ReturnCodes.OK, replyBody);
        }
    }

    private void handleUpdateLedger(JsonNode msg) throws JsonProcessingException {
        Session session = verifySession(msg);
        if (session.isValid()) {
            JsonNode body = msg.get(Fields.BODY);
            long version = body.get(Fields.VERSION).asInt();
            LedgerMetadata updatedMd = mapper.treeToValue(body.get("ledger_metadata"), LedgerMetadata.class);
            Versioned<LedgerMetadata> vUpdatedMd = new Versioned<>(updatedMd, version);

            Versioned<LedgerMetadata> vCurrentMd = ledgers.get(updatedMd.getLedgerId());
            if (vCurrentMd == null) {
                logger.logDebug("Rejected ledger update due to NO_SUCH_LEDGER. Ledger: " + updatedMd.getLedgerId()
                        + " from: " + msg.get(Fields.SOURCE).asText());
                reply(msg, ReturnCodes.Metadata.NO_SUCH_LEDGER);
                return;
            }

            if (vUpdatedMd.getVersion() != vCurrentMd.getVersion()) {
                logger.logDebug("Rejected ledger update due to BAD_VERSION. Ledger: " + updatedMd.getLedgerId()
                        + " from: " + msg.get(Fields.SOURCE).asText());
                reply(msg, ReturnCodes.Metadata.BAD_VERSION);
                return;
            }

            vUpdatedMd.incrementVersion();
            ledgers.put(updatedMd.getLedgerId(), vUpdatedMd);

            logger.logDebug("Accepted ledger update. Ledger: " + updatedMd
                    + " from: " + msg.get(Fields.SOURCE).asText());

            ObjectNode replyBody = mapper.createObjectNode();
            replyBody.put(Fields.SESSION_ID, session.getSessionId());
            replyBody.put(Fields.L.LEDGER_ID, updatedMd.getLedgerId());
            replyBody.put(Fields.VERSION, vUpdatedMd.getVersion());
            replyBody.set(Fields.M.LEDGER_METADATA, mapper.readTree(mapper.writeValueAsString(updatedMd)));
            reply(msg, ReturnCodes.OK, replyBody);
        }
    }

    private void handleGetLedgerId(JsonNode msg) {
        Session session = verifySession(msg);
        if (session.isValid()) {
            nextLedgerId++;
            ObjectNode replyBody = mapper.createObjectNode();
            replyBody.put(Fields.SESSION_ID, session.getSessionId());
            replyBody.put(Fields.L.LEDGER_ID, nextLedgerId);
            reply(msg, ReturnCodes.OK, replyBody);
        }
    }

    private void handleCreateLedger(JsonNode msg) throws JsonProcessingException {
        Session session = verifySession(msg);
        if (session.isValid()) {
            JsonNode body = msg.get(Fields.BODY);
            LedgerMetadata ledgerMd = mapper.treeToValue(body.get(Fields.M.LEDGER_METADATA), LedgerMetadata.class);
            Versioned<LedgerMetadata> vLedgerMd = new Versioned<>(ledgerMd, 0);
            ledgers.put(ledgerMd.getLedgerId(), vLedgerMd);

            logger.logDebug("Accepted ledger create. Ledger: " + ledgerMd
                    + " from: " + msg.get(Fields.SOURCE).asText());

            ObjectNode res = mapper.createObjectNode();
            res.put(Fields.VERSION, 0);
            res.set(Fields.M.LEDGER_METADATA, mapper.valueToTree(ledgerMd));
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

                logger.logDebug("Accepted ledger list update. versionFrom: " + version
                        + " versionTo: " + ledgerList.getVersion()
                        + " listFrom:" + ledgerList.getValue()
                        + " listTo: " + updatedLedgerList
                        + " from: " + msg.get(Fields.SOURCE).asText());

                ledgerList.setValue(updatedLedgerList);

                ObjectNode replyBody = mapper.createObjectNode();
                replyBody.put(Fields.KV.LEDGER_LIST_VERSION, ledgerList.getVersion());
                replyBody.set(Fields.KV.LEDGER_LIST, mapper.valueToTree(ledgerList.getValue()));
                reply(msg, ReturnCodes.OK, replyBody);
            } else {
                logger.logDebug("Rejected ledger list update. versionRejected: " + version
                        + " versionCurrent: " + ledgerList.getVersion()
                        + " listRejected:" + updatedLedgerList
                        + " listCurrent: " + ledgerList.getValue()
                        + " from: " + msg.get(Fields.SOURCE).asText());

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

    private boolean mayBeRedirect(JsonNode request) {
        String type = request.get(Fields.BODY).get(Fields.MSG_TYPE).asText();
        if (Constants.KvStore.Ops.Types.contains(type)) {
            proxy(request, Node.getKvStoreNode());
            return true;
        } else {
            return false;
        }
    }
}
