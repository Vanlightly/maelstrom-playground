package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.Logger;
import com.vanlightly.bookkeeper.MessageSender;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerHandle;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public abstract class LogClient {
    protected MetadataManager metadataManager;
    protected LedgerManager ledgerManager;
    protected ObjectMapper mapper;
    protected Logger logger;
    protected MessageSender messageSender;
    protected AtomicBoolean isCancelled;

    protected BiConsumer<Position, Op> cursorUpdater;
    protected LedgerHandle lh;

    public LogClient(MetadataManager metadataManager,
                     LedgerManager ledgerManager,
                     ObjectMapper mapper,
                     Logger logger,
                     MessageSender messageSender,
                     BiConsumer<Position, Op> cursorUpdater) {
        this.metadataManager = metadataManager;
        this.ledgerManager = ledgerManager;
        this.mapper = mapper;
        this.logger = logger;
        this.messageSender = messageSender;
        this.cursorUpdater = cursorUpdater;
        this.isCancelled = new AtomicBoolean();
    }

    public void cancel() {
        isCancelled.set(true);
    }
}
