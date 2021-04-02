package com.vanlightly.bookkeeper.kv;

import com.vanlightly.bookkeeper.Constants;
import com.vanlightly.bookkeeper.Fields;

import java.util.ArrayList;
import java.util.List;

public class OpLog {
    private List<Op> log;
    private int replicateIndex;
    private int commitIndex;
    private int appliedIndex;

    public OpLog() {
        log = new ArrayList<>();
        replicateIndex = -1;
        commitIndex = -1;
        appliedIndex = -1;
    }

    public void add(Op op) {
        log.add(op);
    }

    public void committed(Op op) {
        if (log.contains(op)) {
            op.setCommitted(true);

            for (int i = 0; i < log.size(); i++) {
                if (log.get(i).isCommitted()) {
                    if (i > commitIndex) {
                        commitIndex = i;
                    }
                }
            }
        } else {
            // error, should not happen
        }

    }

    public boolean hasUnreplicated() {
        return replicateIndex < log.size();
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
