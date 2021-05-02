//package com.vanlightly.bookkeeper.kv.bkclient;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import com.vanlightly.bookkeeper.*;
//import com.vanlightly.bookkeeper.kv.log.Position;
//import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
//import com.vanlightly.bookkeeper.metadata.Versioned;
//import com.vanlightly.bookkeeper.util.Futures;
//
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//public class ReadOp {
//    ObjectMapper mapper;
//    Logger logger;
//    String readCommand;
//    long entryId;
//    LedgerHandle lh;
//    int readCount;
//    boolean requiresQuorum;
//    boolean fence;
//    boolean isRecovery;
//    CompletableFuture<Result<Entry>> callerFuture;
//    AtomicBoolean isCancelled;
//
//    public ReadOp(String readCommand,
//                  long entryId,
//                  boolean requiresQuorum,
//                  boolean fence,
//                  boolean isRecovery,
//                  LedgerHandle lh,
//                  CompletableFuture<Result<Entry>> callerFuture,
//                  ObjectMapper mapper,
//                  Logger logger,
//                  AtomicBoolean isCancelled) {
//        this.readCommand = readCommand;
//        this.entryId = entryId;
//        this.lh = lh;
//        this.requiresQuorum = requiresQuorum;
//        this.fence = fence;
//        this.isRecovery = isRecovery;
//        this.callerFuture = callerFuture;
//        this.isCancelled = isCancelled;
//        this.mapper = mapper;
//        this.logger = logger;
//        this.readCount = 0;
//    }
//
//    public void begin() {
//        ObjectNode readReq = mapper.createObjectNode();
//        readReq.put(Fields.L.LEDGER_ID, lh.getLedgerId());
//
//        int msgTimeout = Constants.Timeouts.TimeoutMs;
//        if (readCommand.equals(Commands.Bookie.READ_LAC_LONG_POLL)) {
//            readReq.put(Fields.L.PREVIOUS_LAC, entryId);
//            readReq.put(Fields.L.LONG_POLL_TIMEOUT_MS, Constants.KvStore.LongPollTimeoutMs);
//            msgTimeout = Constants.KvStore.LongPollResponseTimeoutMs;
//        } else {
//            readReq.put(Fields.L.ENTRY_ID, entryId);
//        }
//
//        if (fence) {
//            readReq.put(Fields.L.FENCE, true);
//        }
//
//        if (isRecovery) {
//            readReq.put(Fields.L.RECOVERY, true);
//        }
//
//        List<String> bookies = lh.getCachedLedgerMetadata().getValue().getCurrentEnsemble();
//        int writeQuorum = lh.getCachedLedgerMetadata().getValue().getWriteQuorum();
//        for (int b=0; b<writeQuorum; b++) {
//            String bookieId = bookies.get(b);
//            messageSender.sendRequest(bookieId, readCommand, readReq, msgTimeout)
//                    .thenApply(this::checkForCancellation)
//                    .thenApply((JsonNode reply) -> {
//                        JsonNode body = reply.get(Fields.BODY);
//                        String code = body.get(Fields.RC).asText();
//                        if (code.equals(ReturnCodes.OK)) {
//                            long lac = body.get(Fields.L.LAC).asLong();
//                            this.updateLac(lac);
//
//                            Entry entry = new Entry(
//                                    body.get(Fields.L.LEDGER_ID).asLong(),
//                                    body.get(Fields.L.ENTRY_ID).asLong(),
//                                    body.get(Fields.L.LAC).asLong(),
//                                    body.path(Fields.L.VALUE).asText());
//                            return new Result<>(code, entry);
//                        } else {
//                            return new Result<>(code, null);
//                        }
//                    })
//                    .thenAccept((JsonNode reply) -> {
//
//                    })
//
//        }
//    }
//
//    private CompletableFuture<Position> readNext(LedgerHandle lh, Position prev) {
//        logger.logDebug("RECOVERY: Last read: " + prev);
//        return lh.parallelRead(prev.getEntryId() + 1, Commands.Bookie.READ_ENTRY,
//                true, false, true)
//                .thenCompose((Result<Entry> result) -> {
//                    if (isCancelled.get()) {
//                        callerFuture.completeExceptionally(new OperationCancelledException());
//                        throw new OperationCancelledException();
//                    } else {
//                        if (result.getCode().equals(ReturnCodes.OK)) {
//                            readCount++;
//
//                            logger.logDebug("RECOVERY: Read " + readCount + " successful " + result.getData() +
//                                    ". Writing entry back to ensemble.");
//
//                            // write back the entry to the ledger to ensure it reaches write quorum
//                            lh.addRecoveryEntry(result.getData().getValue())
//                                    .whenComplete((Entry e2, Throwable t2) -> {
//                                        if (!isCancelled.get()) {
//                                            if (t2 != null) {
//                                                // the write failed, so we need to abort the recovery op
//                                                isCancelled.set(true);
//                                                callerFuture.completeExceptionally(t2);
//                                                return;
//                                            } else {
//                                                // the write completed successfully, if this was the last
//                                                // write then close the ledger
//                                                writeCount++;
//
//                                                logger.logDebug("RECOVERY: Write " + readCount + " successful " + result.getData());
//
//                                                if (readsComplete && readCount == writeCount) {
//                                                    logger.logDebug("RECOVERY: Writes complete");
//                                                    closeLedger();
//                                                }
//                                            }
//                                        }
//                                    });
//
//                            // read the next entry
//                            Position currPos = new Position(result.getData().getLedgerId(),
//                                    result.getData().getEntryId());
//                            return readNext(lh, currPos);
//                        } else if (result.getCode().equals(ReturnCodes.Ledger.NO_QUORUM)) {
//                            // the previous read was the last good entry
//                            return CompletableFuture.completedFuture(prev);
//                        } else {
//                            // we don't know if the current entry exists or not
//                            throw new BkException("Too many unknown responses", ReturnCodes.Ledger.UNKNOWN);
//                        }
//                    }
//                });
//    }
//
//    private void closeLedger() {
//        logger.logDebug("RECOVERY: Closing the ledger");
//        lh.close()
//            .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t2) -> {
//                if (isCancelled.get()) {
//                    callerFuture.completeExceptionally(new OperationCancelledException());
//                } else if (t2 != null) {
//                    logger.logError("RECOVERY: Failed to close the ledger", t2);
//                    callerFuture.completeExceptionally(t2);
//                } else {
//                    logger.logDebug("RECOVERY: Ledger closed");
//                    callerFuture.complete(null);
//                }
//            });
//    }
//}
