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
    long currentEntryId;
    boolean changingEnsemble;
    long lastAddConfirmed;
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
        this.currentEntryId = -1L;
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

    public CompletableFuture<Entry> addEntry(String value) {
        CompletableFuture<Entry> future = new CompletableFuture<>();

        currentEntryId++;
        Entry entry = new Entry(versionedMetadata.getValue().getLedgerId(), currentEntryId, value);
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

    public CompletableFuture<Entry> read(long entryId) {
        CompletableFuture<Entry> future = new CompletableFuture<>();
        FutureRetries.retryTransient(future, () ->
                doRead(entryId, 0, false, false));
        return future;
    }

    public CompletableFuture<Entry> recoveryRead(long entryId) {
        CompletableFuture<Entry> future = new CompletableFuture<>();
        FutureRetries.retryTransient(future, () ->
                doRead(entryId, 0, false, true));
        return future;
    }

    private CompletableFuture<Entry> doRead(long entryId, int bookieIndex,
                                            boolean isRetryable, boolean isRecoveryRead) {
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
                                body.get(Fields.L.VALUE).asText());
                        return CompletableFuture.completedFuture(entry);
                } else {
                    boolean retryable = isRetryable || code.equals(ReturnCodes.TIME_OUT);
                    int nextBookieIndex = bookieIndex + 1;
                    if (nextBookieIndex >= versionedMetadata.getValue().getWriteQuorum()) {
                        // no more bookies to try
                        if (retryable) {
                            return FutureRetries.<Entry>retryableFailedFuture("No success responses, but read can be retried due to timeouts");
                        } else {
                            return FutureRetries.<Entry>nonRetryableFailedFuture(new BkException("No success responses", code));
                        }
                    } else {
                        return doRead(entryId, nextBookieIndex, retryable, isRecoveryRead);
                    }
                }
            });
    }

    public CompletableFuture<Long> readExplicitLac() {
        if (versionedMetadata.getValue().getStatus() == LedgerStatus.CLOSED) {
            lastAddConfirmed = versionedMetadata.getValue().getLastEntryId();
            return CompletableFuture.completedFuture(lastAddConfirmed);
        }

        return doQuorumRead(-1L)
                .thenApply((List<Entry> entries) ->
                        entries.stream().map(x -> x.getEntryId()).max(Long::compare).get());
    }

    private CompletableFuture<List<Entry>> doQuorumRead(long entryId) {
        CompletableFuture<List<Entry>> readFuture = new CompletableFuture<>();

        ObjectNode readReq = mapper.createObjectNode();
        readReq.put(Fields.L.LEDGER_ID, versionedMetadata.getValue().getLedgerId());
        readReq.put(Fields.L.ENTRY_ID, entryId);
        CompletableFuture<Entry>[] futures = new CompletableFuture[versionedMetadata.getValue().getWriteQuorum()];

        List<String> bookies = versionedMetadata.getValue().getCurrentEnsemble();
        int writeQuorum = versionedMetadata.getValue().getWriteQuorum();
        for (int b=0; b<writeQuorum; b++) {
            String bookieId = bookies.get(b);
            CompletableFuture<Entry> future = messageSender.sendRequest(bookieId,
                                                Commands.Bookie.READ_ENTRY, readReq)
                    .thenCompose((JsonNode reply) -> {
                        JsonNode body = reply.get(Fields.BODY);
                        String code = body.get(Fields.RC).asText();
                        if (code.equals(ReturnCodes.OK)) {
                            long lac = body.get(Fields.L.LAC).asLong();
                            this.updateLac(lac);
                            Entry entry = new Entry(
                                    body.get(Fields.L.LEDGER_ID).asLong(),
                                    body.get(Fields.L.ENTRY_ID).asLong(),
                                    body.get(Fields.L.VALUE).asText());
                            return CompletableFuture.completedFuture(entry);
                        } else {
                            boolean retryable = code.equals(ReturnCodes.TIME_OUT);
                            if (retryable) {
                                return FutureRetries.<Entry>retryableFailedFuture("Non-success response, but read can be retried due to timeouts");
                            } else {
                                return FutureRetries.<Entry>nonRetryableFailedFuture(new BkException("Non-success response", code));
                            }
                        }
                    });
            futures[b] = future;
        }

        CompletableFuture.allOf(futures).
            thenRun(() -> {
                List<Entry> entries = new ArrayList<>();
                int unknown = 0;

                for (int b=0; b<writeQuorum; b++) {
                    CompletableFuture<Entry> future = futures[b];
                    try {
                        Entry entry = future.get();
                        entries.add(entry);
                    } catch (TransientException e) {
                        unknown++;
                    } catch (Exception ignored) {}
                }

                if (unknown >= versionedMetadata.getValue().getAckQuorum()) {
                    readFuture.completeExceptionally(new BkException("Not enough explicit responses", ReturnCodes.Bookie.NOT_ENOUGH_EXPLICIT_RESPONSES));
                } else {
                    readFuture.complete(entries);
                }
            });

        return readFuture;
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
