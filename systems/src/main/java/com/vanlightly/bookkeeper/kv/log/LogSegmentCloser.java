package com.vanlightly.bookkeeper.kv.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.Logger;
import com.vanlightly.bookkeeper.MessageSender;
import com.vanlightly.bookkeeper.kv.Op;
import com.vanlightly.bookkeeper.kv.bkclient.LedgerManager;

import java.util.function.BiConsumer;

public class LogSegmentCloser extends LogClient {
    public LogSegmentCloser(MetadataManager metadataManager,
                            LedgerManager ledgerManager,
                            ObjectMapper mapper,
                            Logger logger,
                            MessageSender messageSender,
                            BiConsumer<Position, Op> cursorUpdater) {
        super(metadataManager, ledgerManager, mapper, logger, messageSender, cursorUpdater);
    }


}
