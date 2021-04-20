package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.*;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerHandle;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;
import com.vanlightly.bookkeeper.util.Futures;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/*
    Super class of all classes that interact with the log.
    It has its own managers and own cancellation.
 */
public abstract class LogClient {
    protected MetadataManager metadataManager;
    protected LedgerManager ledgerManager;
    protected ObjectMapper mapper;
    protected Logger logger;
    protected MessageSender messageSender;
    protected AtomicBoolean isCancelled;

    protected BiConsumer<Position, Op> cursorUpdater;
    protected LedgerHandle lh;

    public LogClient(ManagerBuilder managerBuilder,
                     ObjectMapper mapper,
                     Logger logger,
                     MessageSender messageSender,
                     BiConsumer<Position, Op> cursorUpdater) {
        this.isCancelled = new AtomicBoolean();
        this.metadataManager = managerBuilder.buildMetadataManager(messageSender, isCancelled);
        this.ledgerManager = managerBuilder.buildLedgerManager(messageSender, isCancelled);
        this.mapper = mapper;
        this.logger = logger;
        this.messageSender = messageSender;
        this.cursorUpdater = cursorUpdater;
    }

    public void cancel() {
        isCancelled.set(true);
    }

    protected <T> T checkForCancellation(T t) {
        if (isCancelled.get()) {
            throw new OperationCancelledException();
        }

        return t;
    }

    protected boolean isCancelled() {
        return isCancelled.get();
    }

    protected boolean isError(Throwable t) {
        return t != null && !(t instanceof OperationCancelledException)
                && !(Futures.unwrap(t) instanceof OperationCancelledException);
    }
}
