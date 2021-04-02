package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;
import com.vanlightly.bookkeeper.kv.log.MetadataManager;

import java.util.concurrent.atomic.AtomicBoolean;

public interface ManagerBuilder {
    SessionManager buildSessionManagerWithKeepAlives(Long keepAliveMs, MessageSender messageSender);
    SessionManager buildSessionManagerWithoutKeepAlives(MessageSender messageSender);
    LedgerManager buildLedgerManager(SessionManager sessionManager, MessageSender messageSender);
    MetadataManager buildMetadataManager(SessionManager sessionManager, MessageSender messageSender);
}

class ManagerBuilderImpl implements ManagerBuilder {
    ObjectMapper mapper;
    Logger logger;
    AtomicBoolean isCancelled;

    public ManagerBuilderImpl(ObjectMapper mapper, Logger logger, AtomicBoolean isCancelled) {
        this.mapper = mapper;
        this.logger = logger;
        this.isCancelled = isCancelled;
    }

    @Override
    public SessionManager buildSessionManagerWithKeepAlives(Long keepAliveMs, MessageSender messageSender) {
        return new SessionManager(keepAliveMs, true,
                mapper, messageSender, logger, isCancelled);
    }

    @Override
    public SessionManager buildSessionManagerWithoutKeepAlives(MessageSender messageSender) {
        return new SessionManager(0L, false,
                mapper, messageSender, logger, isCancelled);
    }

    @Override
    public LedgerManager buildLedgerManager(SessionManager sessionManager,
                                            MessageSender messageSender) {
        return new LedgerManager(mapper, sessionManager, messageSender, logger, isCancelled);
    }

    @Override
    public MetadataManager buildMetadataManager(SessionManager sessionManager,
                                                MessageSender messageSender) {
        return new MetadataManager(mapper, sessionManager, messageSender, logger, isCancelled);
    }
};
