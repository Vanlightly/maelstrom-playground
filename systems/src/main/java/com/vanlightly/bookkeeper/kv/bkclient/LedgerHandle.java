package com.vanlightly.bookkeeper.kv.bkclient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.LedgerStatus;
import com.vanlightly.bookkeeper.metadata.Versioned;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class LedgerHandle {
    private ObjectMapper mapper;
    private Logger logger;
    private MessageSender messageSender;
    private AtomicBoolean isCancelled;

    Versioned<LedgerMetadata> versionedMetadata;
    Queue<PendingAddOp> pendingAddOps;
    Map<Integer, String> delayedWriteFailedBookies;
    long pendingAddsSequenceHead;
    long lastAddPushed;
    long lastAddConfirmed;
    boolean changingEnsemble;
    LedgerManager ledgerManager;

    boolean pendingClose;
    List<CompletableFuture<Versioned<LedgerMetadata>>> closeFutures;

    public LedgerHandle(ObjectMapper mapper,
                        LedgerManager ledgerManager,
                        MessageSender messageSender,
                        Logger logger,
                        AtomicBoolean isCancelled,
                        Versioned<LedgerMetadata> versionedMetadata) {
        this.mapper = mapper;
        this.ledgerManager = ledgerManager;
        this.messageSender = messageSender;
        this.logger = logger;
        this.isCancelled = isCancelled;
        this.versionedMetadata = versionedMetadata;
        this.pendingAddOps = new ArrayDeque<>();
        this.pendingAddsSequenceHead = -1L;
        this.lastAddConfirmed = -1L;
        this.lastAddPushed = -1L;
        this.delayedWriteFailedBookies = new HashMap<>();
        this.closeFutures = new ArrayList<>();
    }

    public Versioned<LedgerMetadata> getCachedLedgerMetadata() {
        return versionedMetadata;
    }

    public void setCachedLedgerMetadata(Versioned<LedgerMetadata> updatedMetadata) {
        versionedMetadata = updatedMetadata;
    }

    public long getLedgerId() {
        return versionedMetadata.getValue().getLedgerId();
    }

    public long getLastAddConfirmed() {
        return lastAddConfirmed;
    }

    public long getLastAddPushed() {
        return lastAddPushed;
    }

    public void setLastAddPushed(long lastAddPushed) {
        this.lastAddPushed = lastAddPushed;
    }

    public void setLastAddConfirmed(long lastAddConfirmed) {
        this.lastAddConfirmed = lastAddConfirmed;
    }

    public CompletableFuture<Entry> addEntry(String value) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        lastAddPushed++;
        Entry entry = new Entry(versionedMetadata.getValue().getLedgerId(), lastAddPushed, value);
        PendingAddOp addOp = new PendingAddOp(mapper,
                messageSender,
                entry,
                versionedMetadata.getValue().getCurrentEnsemble(),
                versionedMetadata.getValue().getWriteQuorum(),
                versionedMetadata.getValue().getAckQuorum(),
                this,
                future);

        addOp.begin();
        pendingAddOps.add(addOp);

        return future;
    }

    public CompletableFuture<Result<Entry>> read(long entryId) {
        return doRead(entryId, 0, false, 0);
    }

    public CompletableFuture<Result<Entry>> recoveryRead(long entryId) {
        return doRead(entryId, 0, true, 0);
    }

    /*
        Performs a read against a single bookie. If a non-success response is received, it
        sequentially tries the next bookie until either a success response or there are no
        more bookies left to try
     */
    private CompletableFuture<Result<Entry>> doRead(long entryId,
                                                    int bookieIndex,
                                                    boolean isRecoveryRead,
                                                    final int unknown) {
        ObjectNode readReq = mapper.createObjectNode();
        readReq.put(Fields.L.LEDGER_ID, versionedMetadata.getValue().getLedgerId());
        readReq.put(Fields.L.ENTRY_ID, entryId);
        readReq.put(Fields.L.RECOVERY, isRecoveryRead);
        readReq.put(Fields.L.FENCE, isRecoveryRead);
        String bookieId = versionedMetadata.getValue().getCurrentEnsemble().get(bookieIndex);

        return messageSender.sendRequest(bookieId, Commands.Bookie.READ_ENTRY, readReq)
            .thenCompose((JsonNode reply) -> {
                JsonNode body = reply.get(Fields.BODY);
                String code = body.get(Fields.RC).asText();
                if (code.equals(ReturnCodes.OK)) {
                        long lac = body.get(Fields.L.LAC).asLong();

                        if (!isRecoveryRead) {
                            this.updateLac(lac);
                        }

                        Entry entry = new Entry(
                                body.get(Fields.L.LEDGER_ID).asLong(),
                                body.get(Fields.L.ENTRY_ID).asLong(),
                                body.get(Fields.L.LAC).asLong(),
                                body.get(Fields.L.VALUE).asText());
                        return CompletableFuture.completedFuture(new Result<>(ReturnCodes.OK, entry));
                } else {
                    int unknown1 = unknown;
                    if (!code.equals(ReturnCodes.Bookie.NO_SUCH_ENTRY)
                            && !code.equals(ReturnCodes.Bookie.NO_SUCH_LEDGER)) {
                        unknown1++;
                    }

                    int nextBookieIndex = bookieIndex + 1;
                    if (nextBookieIndex >= versionedMetadata.getValue().getWriteQuorum()) {
                        if (unknown1 >= 0) {
                            return CompletableFuture.completedFuture(new Result<>(ReturnCodes.Ledger.UNKNOWN, null));
                        } else {
                            return CompletableFuture.completedFuture(new Result<>(code, null));
                        }
                    } else {
                        return doRead(entryId, nextBookieIndex, isRecoveryRead, unknown1);
                    }
                }
            });
    }

    public CompletableFuture<Result<Entry>> readLac() {
        return parallelRead(-1L, Commands.Bookie.READ_LAC, true);
    }

    public CompletableFuture<Result<Entry>> lacLongPollRead() {
        return parallelRead(lastAddConfirmed, Commands.Bookie.READ_LAC_LONG_POLL, false);
    }

    /*
        Sends a read all the whole ensemble in parallel and returns the entry with the highest LAC
        as long as enough bookies (AckQuorum) respond positively.
     */
    public CompletableFuture<Result<Entry>> parallelRead(long entryId, String readCommand,
                                                         boolean requiresQuorum) {
        CompletableFuture<Result<Entry>> readFuture = new CompletableFuture<>();

        ObjectNode readReq = mapper.createObjectNode();
        readReq.put(Fields.L.LEDGER_ID, versionedMetadata.getValue().getLedgerId());

        if (readCommand.equals(Commands.Bookie.READ_LAC_LONG_POLL)) {
            readReq.put(Fields.L.PREVIOUS_LAC, entryId);
            readReq.put(Fields.L.LONG_POLL_TIMEOUT_MS, Constants.KvStore.LongPollTimeoutMs);
        } else {
            readReq.put(Fields.L.ENTRY_ID, entryId);
        }

        CompletableFuture<Result<Entry>>[] futures = new CompletableFuture[versionedMetadata.getValue().getWriteQuorum()];

        List<String> bookies = versionedMetadata.getValue().getCurrentEnsemble();
        int writeQuorum = versionedMetadata.getValue().getWriteQuorum();
        for (int b=0; b<writeQuorum; b++) {
            String bookieId = bookies.get(b);
            CompletableFuture<Result<Entry>> future =
                    messageSender.sendRequest(bookieId, readCommand, readReq,
                            Constants.KvStore.LongPollTimeoutMs*3)
                    .thenApply((JsonNode reply) -> {
                        JsonNode body = reply.get(Fields.BODY);
                        String code = body.get(Fields.RC).asText();
                        if (code.equals(ReturnCodes.OK)) {
                            long lac = body.get(Fields.L.LAC).asLong();
                            this.updateLac(lac);

                            Entry entry = new Entry(
                                    body.get(Fields.L.LEDGER_ID).asLong(),
                                    body.get(Fields.L.ENTRY_ID).asLong(),
                                    body.get(Fields.L.LAC).asLong(),
                                    body.path(Fields.L.VALUE).asText());
                            return new Result<>(code, entry);
                        } else {
                            return new Result<>(code, null);
                        }
                    });
            futures[b] = future;
        }

        CompletableFuture<Void> allWithFailFast = CompletableFuture.allOf(futures);
        Arrays.stream(futures)
                .forEach(f -> f.exceptionally(e -> {
                    allWithFailFast.completeExceptionally(e);
                    return null;
                }));

        allWithFailFast.
            whenComplete((Void v, Throwable t) -> {
                if (t != null) {
                    readFuture.completeExceptionally(t);
                    return;
                }

                int unknown = 0;
                int positive = 0;
                int negative = 0;
                Entry entry = null;

                for (int b=0; b<writeQuorum; b++) {
                    CompletableFuture<Result<Entry>> future = futures[b];
                    try {
                        Result<Entry> result = future.get();
                        if (result.getCode().equals(ReturnCodes.OK)) {
                            // return the entry with the highest LAC
                            if (entry == null) {
                                entry = result.getData();
                            } else if (entry.getLac() < result.getData().getLac()) {
                                entry = result.getData();
                            }
                            positive++;
                        } else if (result.getCode().equals(ReturnCodes.Bookie.NO_SUCH_LEDGER) ||
                                result.getCode().equals(ReturnCodes.Bookie.NO_SUCH_ENTRY)) {
                            negative++;
                        } else {
                            unknown++;
                        }
                    } catch (Throwable t2) {
                        logger.logError("Failed read", t2);
                        unknown++;
                    }
                }

                int required = 1;
                if (requiresQuorum) {
                    required = minForAckQuorum(versionedMetadata.getValue());
                }

                if (positive >= required) {
                    readFuture.complete(new Result<>(ReturnCodes.OK, entry));
                } else if (unknown >= versionedMetadata.getValue().getAckQuorum()) {
                    readFuture.complete(new Result<>(ReturnCodes.Ledger.UNKNOWN, null));
                } else {
                    readFuture.complete(new Result<>(ReturnCodes.Ledger.NO_QUORUM, null));
                }
            });

        return readFuture;
    }

    private int minForAckQuorum(LedgerMetadata lm) {
        return (lm.getWriteQuorum() - lm.getAckQuorum()) + 1;
    }

    void handleUnrecoverableErrorDuringAdd(String rc) {
        closeInternal(rc);
    }

    void updateLac(long entryId) {
        if (entryId > lastAddConfirmed) {
            lastAddConfirmed = entryId;
        }
    }

    public CompletableFuture<Versioned<LedgerMetadata>> close() {
        if (pendingClose) {
            CompletableFuture<Versioned<LedgerMetadata>> future = new CompletableFuture<>();
            closeFutures.add(future);
            return future;
        }

        return closeInternal(ReturnCodes.Ledger.LEDGER_CLOSED);
    }

    private CompletableFuture<Versioned<LedgerMetadata>> closeInternal(String rc) {
        pendingClose = true;
        versionedMetadata.getValue().setStatus(LedgerStatus.CLOSED);
        versionedMetadata.getValue().setLastEntryId(lastAddConfirmed);
        errorOutPendingAdds(rc);
        return ledgerManager.updateLedgerMetadata(versionedMetadata)
                .whenComplete((Versioned<LedgerMetadata> vlm, Throwable t) -> {
                    for (CompletableFuture<Versioned<LedgerMetadata>> future : closeFutures) {
                        if (t != null) {
                            future.completeExceptionally(t);
                        } else {
                            future.complete(vlm);
                        }
                    }
                });
    }

    private void errorOutPendingAdds(String rc) {
        errorOutPendingAdds(rc, drainPendingAdds());
    }

    private List<PendingAddOp> drainPendingAdds() {
        PendingAddOp pendingAddOp;
        List<PendingAddOp> opsDrained = new ArrayList<>(pendingAddOps.size());
        while ((pendingAddOp = pendingAddOps.poll()) != null) {
            opsDrained.add(pendingAddOp);
        }
        return opsDrained;
    }

    private void errorOutPendingAdds(String rc, List<PendingAddOp> ops) {
        for (PendingAddOp op : ops) {
            op.completeCallerFuture(rc);
        }
    }

    void sendAddSuccessCallbacks() {
        PendingAddOp pendingAddOp;

        while ((pendingAddOp = pendingAddOps.peek()) != null
                && !changingEnsemble) {
            if (!pendingAddOp.isCommitted()) {
                return;
            }

            pendingAddOps.remove(pendingAddOp);
            pendingAddsSequenceHead = pendingAddOp.getEntry().getEntryId();
            lastAddConfirmed = pendingAddsSequenceHead;
            pendingAddOp.completeCallerFuture(ReturnCodes.OK);
        }
    }

    void handleBookieFailure(Map<Integer, String> failedBookies) {
        if (changingEnsemble) {
            delayedWriteFailedBookies.putAll(failedBookies);
        } else {
            changeEnsemble(failedBookies);
        }
    }

    private CompletableFuture<Void> changeEnsemble(Map<Integer, String> failedBookies) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        changingEnsemble = true;

        // work on a copy and replace it at the end
        Versioned<LedgerMetadata> cachedMetadata = new Versioned<>(
                new LedgerMetadata(versionedMetadata.getValue()), versionedMetadata.getVersion());

        if (cachedMetadata.getValue().getStatus() == LedgerStatus.CLOSED) {
            future.complete(null);
        } else {
            Map<Integer, String> bookiesToReplace = new HashMap<>(delayedWriteFailedBookies);
            bookiesToReplace.putAll(failedBookies);

            List<String> newEnsemble = new ArrayList<>(cachedMetadata.getValue().getCurrentEnsemble());
            ledgerManager.getAvailableBookies()
                .whenComplete((List<String> availableBookies, Throwable bookiesThrowable) -> {
                    if (bookiesThrowable != null) {
                        // handle
                    } else {
                        availableBookies.removeAll(newEnsemble);
                        if (availableBookies.size() < bookiesToReplace.size()) {
                            logger.logError("Couldn't add a new ensemble, not enough bookies");
                            closeInternal(ReturnCodes.Bookie.NOT_ENOUGH_BOOKIES)
                                    .whenComplete((Versioned<LedgerMetadata> md, Throwable closeThrowable) ->
                                    {
                                        if (closeThrowable != null) {
                                            logger.logError("Failed closing the ledger", closeThrowable);
                                        }
                                        changingEnsemble = false;
                                        future.complete(null);
                                        // TODO what state should this be left in?
                                    });
                        } else {
                            Collections.shuffle(availableBookies);
                            Set<Integer> replacedBookieIndices = new HashSet<>();

                            for (int bookieIndex : bookiesToReplace.keySet()) {
                                String newBookie = availableBookies.get(0);
                                newEnsemble.set(bookieIndex, newBookie);
                                replacedBookieIndices.add(bookieIndex);
                            }

                            cachedMetadata.getValue().replaceCurrentEnsemble(newEnsemble);

                            ledgerManager.updateLedgerMetadata(cachedMetadata)
                                    .whenComplete((Versioned<LedgerMetadata> md, Throwable updateThrowable) ->
                                    {
                                        if (updateThrowable != null) {
                                            // handle
                                            if (updateThrowable.getCause() instanceof MetadataException) {
                                                MetadataException me = (MetadataException)updateThrowable.getCause();

                                                if (me.getCode().equals(ReturnCodes.Metadata.BAD_VERSION)) {
                                                    // another process has updated the ledger metadata, so our copy is now stale
                                                    errorOutPendingAdds(ReturnCodes.Metadata.BAD_VERSION);
                                                    changingEnsemble = false;
                                                }
                                            } else {
                                                // TODO
                                            }
                                        } else {
                                            versionedMetadata = md;
                                            unsetSuccessAndSendWriteRequest(newEnsemble, replacedBookieIndices);

                                            for (Integer replacedBookie : replacedBookieIndices) {
                                                delayedWriteFailedBookies.remove(replacedBookie);
                                            }
                                        }
                                        changingEnsemble = false;

                                        future.complete(null);
                                    });
                        }
                    }
                });
        }

        return future;
    }

    void unsetSuccessAndSendWriteRequest(List<String> ensemble, final Set<Integer> replacedBookieIndices) {
        for (PendingAddOp pendingAddOp : pendingAddOps) {
            for (Integer bookieIndex : replacedBookieIndices) {
                pendingAddOp.unsetSuccessAndSendWriteRequest(ensemble, bookieIndex);
            }
        }
    }
}
