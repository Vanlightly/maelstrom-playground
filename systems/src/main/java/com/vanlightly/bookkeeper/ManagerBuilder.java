package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;
import com.vanlightly.bookkeeper.kv.log.MetadataManager;

import java.util.concurrent.atomic.AtomicBoolean;

public interface ManagerBuilder {
    SessionManager buildSessionManager(Long keepAliveMs,
                                       MessageSender messageSender);
    LedgerManager buildLedgerManager(MessageSender messageSender,
                                     AtomicBoolean isCancelled);
    MetadataManager buildMetadataManager(MessageSender messageSender,
                                         AtomicBoolean isCancelled);
}

class ManagerBuilderImpl implements ManagerBuilder {
    ObjectMapper mapper;
    Logger logger;
    SessionManager sessionManager;

    public ManagerBuilderImpl(ObjectMapper mapper, Logger logger) {
        this.mapper = mapper;
        this.logger = logger;
    }

    @Override
    public SessionManager buildSessionManager(Long keepAliveMs,
                                              MessageSender messageSender) {
        sessionManager = new SessionManager(keepAliveMs, mapper, messageSender, logger);
        return sessionManager;
    }

    @Override
    public LedgerManager buildLedgerManager(MessageSender messageSender,
                                            AtomicBoolean isCancelled) {
        return new LedgerManager(mapper, sessionManager, messageSender, logger, isCancelled);
    }

    @Override
    public MetadataManager buildMetadataManager(MessageSender messageSender,
                                                AtomicBoolean isCancelled) {
        return new MetadataManager(mapper, sessionManager, messageSender, logger, isCancelled);
    }
};
