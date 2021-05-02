package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.bookie.Entry;
import com.vanlightly.bookkeeper.bookie.Ledger;
import com.vanlightly.bookkeeper.network.NetworkIO;
import com.vanlightly.bookkeeper.util.InvariantViolationException;
import com.vanlightly.bookkeeper.util.Logger;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;

public class BookieNode extends Node {
    Map<Long, Ledger> ledgers;

    private Instant lastCheckedExpiredReads;

    public BookieNode(String nodeId,
                      NetworkIO net,
                      ObjectMapper mapper,
                      ManagerBuilder builder) {
        super(nodeId, true, net, mapper, builder);
        ledgers = new HashMap<>();
        lastCheckedExpiredReads = Instant.now().minus(1, ChronoUnit.DAYS);
    }

    @Override
    void initialize(JsonNode initMsg) {
        sendInitOk(initMsg);
    }

    @Override
    boolean roleSpecificAction() {
        return sessionManager.maintainSession()
                || expireLongPollLacReads();
    }

    @Override
    void handleRequest(JsonNode request) {
        //logger.logDebug("Received request: " + request.toString());
        if (mayBeRedirect(request)) {
            return;
        }

        String type = request.get(Fields.BODY).get(Fields.MSG_TYPE).asText();

        if (sessionManager.handlesRequest(type)) {
            sessionManager.handleRequest(request);
        } else {
            switch (type) {
                case Commands.PRINT_STATE:
                    printState();
                    break;
                case Commands.Bookie.ADD_ENTRY:
                    handleAddEntry(request);
                    break;
                case Commands.Bookie.READ_ENTRY:
                    handleReadEntry(request);
                    break;
                case Commands.Bookie.READ_LAC:
                    handleReadLac(request);
                    break;
                case Commands.Bookie.READ_LAC_LONG_POLL:
                    handleReadLacLongPoll(request);
                    break;
                default:
                    logger.logError("Bad command type: " + type);
            }
        }
    }

    void printState() {
        logger.logInfo("----------- Bookie State --------------------");
        logger.logInfo("Ledgers:");
        for (Map.Entry<Long,Ledger> ledger : ledgers.entrySet()) {
            logger.logInfo(ledger.getValue().toString());
        }
        logger.logInfo("---------------------------------------------");
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

    private void handleAddEntry(JsonNode msg) {
        if (!checkLedgerWritable(msg)) {
            return;
        }

        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
        long entryId = body.get(Fields.L.ENTRY_ID).asLong();
        long lac = body.get(Fields.L.LAC).asLong();
        String value = body.get(Fields.L.VALUE).asText();

        Ledger ledger = getLedger(ledgerId);
        ledger.add(entryId, lac, value);
        reply(msg, ReturnCodes.OK);

        logger.logDebug("ADD for ledger: " + ledgerId + " entry: " + entryId
                + " from: " + msg.get(Fields.SOURCE).asText());

        checkLocalInvariants();
    }

    private void handleReadEntry(JsonNode msg) {
        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
        long entryId = body.get(Fields.L.ENTRY_ID).asLong();
        boolean isFencingRead = msg.get(Fields.BODY).path(Fields.L.FENCE).asBoolean(false);

        if (isFencingRead) {
            logger.logDebug("Fenced ledger: " + ledgerId + " from: " + msg.get(Fields.SOURCE).asText());
            fenceLedger(ledgerId);
        }

        Ledger ledger = getReadOnlyLedger(ledgerId);

        ObjectNode res = mapper.createObjectNode();
        res.put(Fields.L.LEDGER_ID, ledgerId);

        if (ledger == null) {
            logger.logDebug("READ (NO_SUCH_LEDGER) for ledger: " + ledgerId + " entry: " + entryId
                    + " from: " + msg.get(Fields.SOURCE).asText());
            reply(msg, ReturnCodes.Bookie.NO_SUCH_LEDGER, res);
        } else if (entryId == -1L) {
            res.put(Fields.L.ENTRY_ID, entryId);
            res.put(Fields.L.LAC, ledger.getLac());

            logger.logDebug("READ LAC " + ledger.getLac() + " (OK) for ledger: " + ledgerId + " entry: " + entryId
                    + " from: " + msg.get(Fields.SOURCE).asText());

            reply(msg, ReturnCodes.OK, res);
        } else if (ledger.hasEntry(entryId)) {
            res.put(Fields.L.ENTRY_ID, entryId);
            res.put(Fields.L.LAC, ledger.getLac());
            res.put(Fields.L.VALUE, ledger.read(entryId));

            logger.logDebug("READ (OK) for ledger: " + ledgerId + " entry: " + entryId
                    + " from: " + msg.get(Fields.SOURCE).asText());

            reply(msg, ReturnCodes.OK, res);
        } else {
            logger.logDebug("READ (NO_SUCH_ENTRY) for ledger: " + ledgerId + " entry: " + entryId
                    + " from: " + msg.get(Fields.SOURCE).asText());

            res.put(Fields.L.ENTRY_ID, entryId);
            reply(msg, ReturnCodes.Bookie.NO_SUCH_ENTRY, res);
        }
    }

    private void handleReadLac(JsonNode msg) {
        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
        boolean isFencingRead = msg.get(Fields.BODY).path(Fields.L.FENCE).asBoolean(false);

        ObjectNode res = mapper.createObjectNode();
        res.put(Fields.L.LEDGER_ID, ledgerId);

        if (isFencingRead) {
            logger.logDebug("Fenced ledger: " + ledgerId + " from: " + msg.get(Fields.SOURCE).asText());
            fenceLedger(ledgerId);
        }

        Ledger ledger = getReadOnlyLedger(ledgerId);

        if (ledger == null) {
            logger.logDebug("READ LAC (NO_SUCH_LEDGER) for ledger: " + ledgerId
                    + " from: " + msg.get(Fields.SOURCE).asText());
            reply(msg, ReturnCodes.Bookie.NO_SUCH_LEDGER, res);
        } else {
            res.put(Fields.L.ENTRY_ID, -1L);
            res.put(Fields.L.LAC, ledger.getLac());

            logger.logDebug("READ LAC " + ledger.getLac() + " (OK) for ledger: " + ledgerId
                    + " from: " + msg.get(Fields.SOURCE).asText());

            reply(msg, ReturnCodes.OK, res);
        }
    }

    private void handleReadLacLongPoll(JsonNode msg) {
        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
        long previousLac = body.get(Fields.L.PREVIOUS_LAC).asLong();
        int timeoutMs = body.get(Fields.L.LONG_POLL_TIMEOUT_MS).asInt();

        Ledger ledger = getReadOnlyLedger(ledgerId);

        if (ledger == null) {
            logger.logDebug("READ LAC (NO_SUCH_LEDGER) for ledger: " + ledgerId
                    + " from: " + msg.get(Fields.SOURCE).asText());

            ObjectNode res = mapper.createObjectNode();
            res.put(Fields.L.LEDGER_ID, ledgerId);
            reply(msg, ReturnCodes.Bookie.NO_SUCH_LEDGER, res);
        } else {
            logger.logDebug("LONG POLL READ LAC for ledger: " + ledgerId
                    + " previousLac: " + previousLac
                    + " from: " + msg.get(Fields.SOURCE).asText());

            CompletableFuture<Entry> future = new CompletableFuture<>();
            ledger.addLacFuture(previousLac, timeoutMs, future);

            future.thenAccept((Entry entry) -> {
                ObjectNode res = mapper.createObjectNode();
                res.put(Fields.L.LEDGER_ID, ledgerId);
                res.put(Fields.L.ENTRY_ID, entry.getEntryId());
                res.put(Fields.L.LAC, entry.getLac());
                if (entry.getValue() != null) {
                    res.put(Fields.L.VALUE, entry.getValue());
                }

                logger.logDebug("LONG POLL READ LAC RESPONSE for ledger: " + ledgerId
                        + " previousLac: " + previousLac
                        + " latestLac: " + entry.getLac()
                        + " from: " + msg.get(Fields.SOURCE).asText());

                reply(msg, ReturnCodes.OK, res);
            })
            .whenComplete((Void v, Throwable t) -> {
                if (t != null) {
                    logger.logError("Long poll read error", t);
                }
            });
        }
    }

    private boolean expireLongPollLacReads() {
        if (Duration.between(lastCheckedExpiredReads, Instant.now()).toMillis() > Constants.Bookie.CheckExpiredLongPollReadsIntervalMs) {
            boolean expired = false;
            for (Ledger ledger : ledgers.values()) {
                if (ledger.expireLacLongPollReads()) {
                    expired = true;
                }
            }
            lastCheckedExpiredReads = Instant.now();

            return expired;
        } else {
            return false;
        }
    }

    private void fenceLedger(long ledgerId) {
        Ledger ledger = getLedger(ledgerId);
        ledger.fenceLedger();
    }

    private boolean checkLedgerWritable(JsonNode msg) {
        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();

        Ledger ledger = getLedger(ledgerId);
        boolean isRecoveryAdd = msg.get(Fields.BODY).path(Fields.L.RECOVERY).asBoolean(false);
        if (ledger.isFenced() && !isRecoveryAdd) {
            logger.logDebug("ADD rejected as ledger is fenced. Ledger: " + ledgerId
                    + " from: " + msg.get(Fields.SOURCE).asText());
            ObjectNode res = mapper.createObjectNode();
            res.put(Fields.L.LEDGER_ID, ledgerId);
            reply(msg, ReturnCodes.Bookie.FENCED, res);
            return false;
        }

        return true;
    }

    private void checkLocalInvariants() {
        for (Ledger ledger : ledgers.values()) {
            for (Map.Entry<Long,String> entry : ledger.getEntries().entrySet()) {
                if (entry.getValue().equals("")) {
                    throw new InvariantViolationException("Empty entry found in ledger: " + ledger.getLedgerId()
                    + " entry: " + entry.getKey());
                }
            }
        }
    }

    private Ledger getLedger(long ledgerId) {
        Ledger ledger = ledgers.get(ledgerId);
        if (ledger == null) {
            ledger = new Ledger(ledgerId);
            ledgers.put(ledgerId, ledger);
        }
        return ledger;
    }

    private Ledger getReadOnlyLedger(long ledgerId) {
        return ledgers.get(ledgerId);
    }
}
