package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.bookie.Entry;
import com.vanlightly.bookkeeper.bookie.Ledger;
import com.vanlightly.bookkeeper.network.NetworkIO;

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
                      Logger logger,
                      ObjectMapper mapper,
                      ManagerBuilder builder) {
        super(nodeId, true, net, logger, mapper, builder);
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

        Ledger ledger = ledgers.get(ledgerId);
        if (ledger == null) {
            ledger = new Ledger(logger, ledgerId);
            ledgers.put(ledgerId, ledger);
        }

        ledger.add(entryId, lac, value);
        reply(msg, ReturnCodes.OK);
    }

    private void handleReadEntry(JsonNode msg) {
        if (!checkLedgerExists(msg)) {
            return;
        }

        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
        long entryId = body.get(Fields.L.ENTRY_ID).asLong();
        boolean isFencingRead = msg.get(Fields.BODY).path(Fields.L.FENCE).asBoolean(false);

        Ledger ledger = ledgers.get(ledgerId);

        if (isFencingRead) {
            ledger.fenceLedger();
        }

        ObjectNode res = mapper.createObjectNode();
        res.put(Fields.L.LEDGER_ID, ledgerId);
        res.put(Fields.L.ENTRY_ID, entryId);
        res.put(Fields.L.LAC, ledger.getLac());

        if (entryId == -1L) {
            reply(msg, ReturnCodes.OK, res);
        } else if (ledger.hasEntry(entryId)) {
            res.put(Fields.L.VALUE, ledger.read(entryId));
            reply(msg, ReturnCodes.OK, res);
        } else {
            reply(msg, ReturnCodes.Bookie.NO_SUCH_ENTRY, res);
        }
    }

    private void handleReadLac(JsonNode msg) {
        if (!checkLedgerExists(msg)) {
            return;
        }

        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
        boolean isFencingRead = msg.get(Fields.BODY).path(Fields.L.FENCE).asBoolean(false);

        ObjectNode res = mapper.createObjectNode();
        res.put(Fields.L.LEDGER_ID, ledgerId);

        Ledger ledger = ledgers.get(ledgerId);
        if (isFencingRead) {
            ledger.fenceLedger();
        }

        res.put(Fields.L.ENTRY_ID, -1L);
        res.put(Fields.L.LAC, ledger.getLac());
        reply(msg, ReturnCodes.OK, res);
    }

    private void handleReadLacLongPoll(JsonNode msg) {
        if (!checkLedgerExists(msg)) {
            return;
        }

        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
        long previousLac = body.get(Fields.L.PREVIOUS_LAC).asLong();
        int timeoutMs = body.get(Fields.L.LONG_POLL_TIMEOUT_MS).asInt();

        Ledger ledger = ledgers.get(ledgerId);
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
            reply(msg, ReturnCodes.OK, res);
        });
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

    private boolean checkLedgerExists(JsonNode msg) {
        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();

        Ledger ledger = ledgers.get(ledgerId);
        if (ledger == null) {
            ObjectNode res = mapper.createObjectNode();
            res.put(Fields.L.LEDGER_ID, ledgerId);
            reply(msg, ReturnCodes.Bookie.NO_SUCH_LEDGER, res);
            return false;
        }

        return true;
    }

    private boolean checkLedgerWritable(JsonNode msg) {
        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();

        Ledger ledger = ledgers.get(ledgerId);

        if (ledger != null) {
            boolean isRecoveryAdd = msg.get(Fields.BODY).path(Fields.L.RECOVERY).asBoolean(false);
            if (ledger.isFenced() && !isRecoveryAdd) {
                ObjectNode res = mapper.createObjectNode();
                res.put(Fields.L.LEDGER_ID, ledgerId);
                reply(msg, ReturnCodes.Bookie.FENCED, res);
                return false;
            }
        }

        return true;
    }
}
