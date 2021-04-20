package com.vanlightly.bookkeeper.kv;

import com.vanlightly.bookkeeper.Logger;
import com.vanlightly.bookkeeper.util.InvariantViolationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpLog {
    private Logger logger;
    private List<Op> log;
    private int replicateIndex;
    private int commitIndex;
    private int appliedIndex;
    private long idSource;

    public OpLog(Logger logger) {
        this.logger = logger;
        log = new ArrayList<>();
        replicateIndex = -1;
        commitIndex = -1;
        appliedIndex = -1;
        idSource = -1;
    }

    public void updateIdSource() {
        if (commitIndex > -1) {
            idSource = log.get(commitIndex).getOpId();
        }
    }

    public void printState() {
        logger.logInfo("----------------- OpLog State -------------");
        logger.logInfo("OpLog. replicateIndex: " + replicateIndex
            + " commitIndex: " + commitIndex
            + " appliedIndex: " + appliedIndex);
        logger.logInfo("Log items:");
        int index = 0;
        for (Op op : log) {
            logger.logInfo(index + " -> " + Op.opToString(op) + " committed: " + op.isCommitted());
            index++;
        }
        logger.logInfo("-------------------------------------------");
    }

    /*
        Used by writers to append new ops that have not yet been replicated
     */
    public void appendNew(Map<String,String> opData) {
        idSource++;
        log.add(new Op(idSource, opData));
        checkInvariants();
    }

    public Op getLastOp() {
        if (log.isEmpty()) {
            return null;
        }

        return log.get(log.size()-1);
    }

    /*
        Used by readers to append committed ops. The commit and
        replicate indices increment in tandem.
     */
    public void appendCommitted(Op op) {
        log.add(op);
        commitIndex++;
        replicateIndex = commitIndex;

        if (commitIndex != log.size()-1) {
            logger.logError("Inconsistent committed index. Is: " + commitIndex
                + " but should be: " + (log.size()-1));
        }
        checkInvariants();
    }

    public void committed(Op op) {
        if (log.contains(op)) {
            int index = log.indexOf(op);
            Op currOp = log.get(index);
            currOp.setCommitted(true);
            logger.logDebug("Committing op: " + Op.opToString(currOp) + " at index: " + index);

            for (int i = 0; i < log.size(); i++) {
                if (i == index) {
                    logger.logDebug("Checking op: " + Op.opToString(currOp) + " at index: " + index);
                }

                if (log.get(i).isCommitted()) {
                    if (i > commitIndex) {
                        logger.logDebug("Advanced commit index from " + commitIndex + " to: " + i);
                        commitIndex = i;
                    }
                }
            }
            checkInvariants();
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

        logger.logInfo("Apply op. replicateIndex: " + replicateIndex
                + " commitIndex: " + commitIndex
                + " appliedIndex: " + appliedIndex);
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
        updateIdSource();

        checkInvariants();

        return uncommitted;
    }

    private void checkInvariants() {
        noUncommittedOpBelowCommittedIndex();
        opIdsAreContiguous();
    }

    private void noUncommittedOpBelowCommittedIndex() {
        for (int i=0; i<commitIndex; i++) {
            if (!log.get(i).isCommitted()) {
                printState();
                throw new InvariantViolationException("An op below the committed index is not in the committed state. Op: " + Op.opToString(log.get(i)));
            }
        }
    }

    private void opIdsAreContiguous() {
        long last = -1;
        for (int i=0; i<commitIndex; i++) {
            long curr = log.get(i).getOpId();
            if (curr != last + 1) {
                printState();
                throw new InvariantViolationException("Op ids are not contiguous.");
            }
            last = curr;
        }
    }
}
