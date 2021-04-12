package com.vanlightly.bookkeeper.kv;

import com.vanlightly.bookkeeper.Logger;

import java.util.ArrayList;
import java.util.List;

public class OpLog {
    private Logger logger;
    private List<Op> log;
    private int replicateIndex;
    private int commitIndex;
    private int appliedIndex;

    public OpLog(Logger logger) {
        this.logger = logger;
        log = new ArrayList<>();
        replicateIndex = -1;
        commitIndex = -1;
        appliedIndex = -1;
    }

    /*
        Used by writers to append new ops that have not yet been replicated
     */
    public void append(Op op) {
        log.add(op);
    }

    /*
        Used by readers to append committed ops
     */
    public void appendCommitted(Op op) {
        log.add(op);
        commitIndex++;

        if (commitIndex != log.size()-1) {
            logger.logError("Inconsistent committed index. Is: " + commitIndex
                + " but should be: " + (log.size()-1));
        }
    }

    public void committed(Op op) {
        if (log.contains(op)) {
            Op currOp = log.get(log.indexOf(op));
            currOp.setCommitted(true);

            for (int i = 0; i < log.size(); i++) {
                if (log.get(i).isCommitted()) {
                    if (i > commitIndex) {
                        commitIndex = i;
                    }
                }
            }
        } else {
            // error, should not happen
            logger.logError("Tried to commit an op that does not exist");
        }

    }

    public boolean hasUnreplicated() {
        return !log.isEmpty() && replicateIndex < log.size() - 1;
    }

    public Op getNextUnreplicatedOp() {
        replicateIndex++;
        return log.get(replicateIndex);
    }

    public boolean hasUnappliedOps() {
        return commitIndex > appliedIndex;
    }

    public Op getNextUnappliedOp() {
        appliedIndex++;
        return log.get(appliedIndex);
    }

    public List<Op> getAllUnappliedOps() {
        List<Op> unapplied = new ArrayList<>();

        for (int i=appliedIndex+1; i<log.size(); i++) {
            unapplied.add(log.get(i));
        }

        return unapplied;
    }

    // TODO: only really need to clear the writes
    public List<Op> clearUncomittedOps() {
        List<Op> newLog = new ArrayList<>();
        List<Op> uncommitted = new ArrayList<>();

        for(int i=0; i<log.size(); i++) {
            if (i <= commitIndex) {
                newLog.add(log.get(i));
            } else {
                uncommitted.add(log.get(i));
            }
        }

        log = newLog;
        replicateIndex = commitIndex;

        return uncommitted;
    }
}
