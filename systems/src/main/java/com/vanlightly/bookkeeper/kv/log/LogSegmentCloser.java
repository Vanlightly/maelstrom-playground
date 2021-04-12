package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.Logger;
import com.vanlightly.bookkeeper.MessageSender;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerHandle;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;
import com.vanlightly.bookkeeper.kv.bkclient.RecoveryOp;
import com.vanlightly.bookkeeper.metadata.LedgerMetadata;
import com.vanlightly.bookkeeper.metadata.Versioned;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class LogSegmentCloser extends LogClient {
    private AtomicBoolean isCancelled;

    public LogSegmentCloser(MetadataManager metadataManager,
                            LedgerManager ledgerManager,
                            ObjectMapper mapper,
                            Logger logger,
                            MessageSender messageSender) {
        super(metadataManager, ledgerManager, mapper, logger, messageSender, null);
        this.isCancelled = new AtomicBoolean(false);
    }

    public CompletableFuture<Void> closeSegment(Position position) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        ledgerManager.getLedgerMetadata(position.getLedgerId())
                .thenApply((Versioned<LedgerMetadata> vlm) -> {
                    return new LedgerHandle(mapper, ledgerManager, messageSender,
                            logger, isCancelled, vlm);
                })
                .thenAccept((LedgerHandle lh) -> {
                    RecoveryOp recoveryOp = new RecoveryOp(lh, future, isCancelled);
                    recoveryOp.begin();
                })
                .whenComplete((Void v, Throwable t) -> {
                    if (t != null) {
                        logger.logError("Failed closing a log segment", t);
                    }
                });

        return future;
    }

    public void cancel() {
        isCancelled.set(true);
    }
}
