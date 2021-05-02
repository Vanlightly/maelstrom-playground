package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;
import com.vanlightly.bookkeeper.kv.log.MetadataManager;
import com.vanlightly.bookkeeper.util.Logger;

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
    SessionManager sessionManager;

    @Override
    public SessionManager buildSessionManager(Long keepAliveMs,
                                              MessageSender messageSender) {
        sessionManager = new SessionManager(keepAliveMs, messageSender);
        return sessionManager;
    }

    @Override
    public LedgerManager buildLedgerManager(MessageSender messageSender,
                                            AtomicBoolean isCancelled) {
        return new LedgerManager(sessionManager, messageSender, isCancelled);
    }

    @Override
    public MetadataManager buildMetadataManager(MessageSender messageSender,
                                                AtomicBoolean isCancelled) {
        return new MetadataManager(sessionManager, messageSender, isCancelled);
    }
};
