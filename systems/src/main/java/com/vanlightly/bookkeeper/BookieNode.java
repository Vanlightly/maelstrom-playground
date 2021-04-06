package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.bookie.Ledger;
import com.vanlightly.bookkeeper.network.NetworkIO;

import java.util.*;

public class BookieNode extends Node {
    Map<Long, Ledger> ledgers;

    public BookieNode(String nodeId,
                      NetworkIO net,
                      Logger logger,
                      ObjectMapper mapper,
                      ManagerBuilder builder) {
        super(nodeId, true, net, logger, mapper, builder);
        ledgers = new HashMap<>();
    }

    @Override
    void initialize(JsonNode initMsg) {
        sendInitOk(initMsg);
    }

    @Override
    boolean roleSpecificAction() {
        return sessionManager.maintainSession();
    }

    @Override
    void handleRequest(JsonNode request) {
        //logger.logDebug("Received request: " + request.toString());
        String type = request.get(Fields.BODY).get(Fields.MSG_TYPE).asText();
        if (sessionManager.handlesRequest(type)) {
            sessionManager.handleRequest(request);
        } else {
            switch (type) {
                case Commands.Bookie.ADD_ENTRY:
                    handleAddEntry(request, false);
                    break;
                case Commands.Bookie.READ_ENTRY:
                    handleReadEntry(request, false);
                    break;
                case Commands.Bookie.READ_LAC:
                    handleReadLac(request);
                    break;
                case Commands.Bookie.RECOVERY_ADD_ENTRY:
                    handleAddEntry(request, true);
                    break;
                case Commands.Bookie.RECOVERY_READ_ENTRY:
                    handleReadEntry(request, true);
                    break;
                default:
                    logger.logError("Bad command type: " + type);
            }
        }
    }

    private void handleAddEntry(JsonNode msg, boolean isRecoveryAdd) {
        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
        long entryId = body.get(Fields.L.ENTRY_ID).asLong();
        long lac = body.get(Fields.L.LAC).asLong();
        String value = body.get(Fields.L.VALUE).asText();

        Ledger ledger = ledgers.get(ledgerId);
        if (ledger == null) {
            ledger = new Ledger();
            ledgers.put(ledgerId, ledger);
        }

        if (ledger.isFenced() && !isRecoveryAdd) {
            reply(msg, ReturnCodes.Bookie.FENCED);
        } else {
            ledger.add(entryId, lac, value);
            reply(msg, ReturnCodes.OK);
        }
    }

    private void handleReadEntry(JsonNode msg, boolean isRecoveryRead) {
        if (!checkLedgerOk(msg, isRecoveryRead)) {
            return;
        }

        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();
        long entryId = body.get(Fields.L.ENTRY_ID).asLong();

        ObjectNode res = mapper.createObjectNode();
        res.put(Fields.L.LEDGER_ID, ledgerId);
        res.put(Fields.L.ENTRY_ID, entryId);

        Ledger ledger = ledgers.get(ledgerId);
        if (ledger.hasEntry(entryId)) {
            res.put(Fields.L.VALUE, ledger.read(entryId));
            reply(msg, ReturnCodes.OK, res);
        } else {
            reply(msg, ReturnCodes.Bookie.NO_SUCH_ENTRY, res);
        }
    }

    private void handleReadLac(JsonNode msg) {
        if (!checkLedgerOk(msg, false)) {
            return;
        }

        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();

        ObjectNode res = mapper.createObjectNode();
        res.put(Fields.L.LEDGER_ID, ledgerId);

        Ledger ledger = ledgers.get(ledgerId);
        res.put(Fields.L.LAC, ledger.getLac());
        reply(msg, ReturnCodes.OK, res);
    }

    private boolean checkLedgerOk(JsonNode msg, boolean isRecoveryOp) {
        JsonNode body = msg.get(Fields.BODY);
        long ledgerId = body.get(Fields.L.LEDGER_ID).asLong();

        Ledger ledger = ledgers.get(ledgerId);
        if (ledger == null) {
            ObjectNode res = mapper.createObjectNode();
            res.put(Fields.L.LEDGER_ID, ledgerId);
            reply(msg, ReturnCodes.Bookie.NO_SUCH_LEDGER, res);
            return false;
        } else if (ledger.isFenced() && !isRecoveryOp) {
            ObjectNode res = mapper.createObjectNode();
            res.put(Fields.L.LEDGER_ID, ledgerId);
            reply(msg, ReturnCodes.Bookie.FENCED, res);
            return false;
        } else {
            return true;
        }
    }
}
