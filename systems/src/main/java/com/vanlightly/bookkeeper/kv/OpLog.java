package com.vanlightly.bookkeeper.kv;

import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;
import com.vanlightly.bookkeeper.util.InvariantViolationException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OpLog {
    private Logger logger = LogManager.getLogger(this.getClass().getSimpleName());
    private List<Op> log;
    private List<Op> tempLog;
    private int replicateIndex;
    private int commitIndex;
    private int appliedIndex;
    private long idSource;

    public OpLog() {
        log = new ArrayList<>();
        tempLog = new ArrayList<>();
        replicateIndex = -1;
        commitIndex = -1;
        appliedIndex = -1;
        idSource = -1;
    }

    public void printState() {
        StringBuilder sb = new StringBuilder();
        sb.append("----------------- OpLog State -------------" + System.lineSeparator());
        sb.append("OpLog. replicateIndex: " + replicateIndex
            + " commitIndex: " + commitIndex
            + " appliedIndex: " + appliedIndex + System.lineSeparator());
        sb.append("Log items:");
        int index = 0;
        for (Op op : log) {
            sb.append(index + " -> " + Op.opToString(op) + " committed: "
                    + op.isCommitted() + System.lineSeparator());
            index++;
        }

        sb.append("Temp Log items:" + System.lineSeparator());
        index = 0;
        for (Op tmpOp : tempLog) {
            sb.append(index + " -> " + Op.opToString(tmpOp) + System.lineSeparator());
            index++;
        }
        sb.append("-------------------------------------------");
        logger.logInfo(sb.toString());
    }

    /*
        Used by writers to append new ops that have not yet been replicated
     */
    public void appendUncommitted(Map<String,String> opData, boolean appendToMain) {
        if (appendToMain) {
            idSource++;
            log.add(new Op(idSource, opData));
            //checkLocalInvariants();
        } else {
            tempLog.add(new Op(-1, opData));
        }
    }

    public void resolveTempLog() {
        logger.logDebug("Resolving temp log. idSource: " + idSource
                + "commitIndex: " + commitIndex + " log size: " + log.size());
        for (Op tmpOp : tempLog) {
            idSource++;
            log.add(new Op(idSource, tmpOp.getFields()));
        }
        tempLog.clear();
        logger.logDebug("Temp log cleared");
        checkLocalInvariants();
    }

    public void discardTempLog() {
        tempLog.clear();
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
        idSource = op.getOpId();
        commitIndex++;
        replicateIndex = commitIndex;

        if (commitIndex != log.size()-1) {
            logger.logError("Inconsistent committed index. Is: " + commitIndex
                + " but should be: " + (log.size()-1));
        } else if (idSource != log.size()-1) {
            logger.logError("Inconsistent idSource. Is: " + idSource
                    + " but should be: " + (log.size() - 1));
        }
        //checkLocalInvariants();
    }

    public void committed(Op op) {
        if (log.contains(op)) {
            int index = log.indexOf(op);
            Op currOp = log.get(index);
            currOp.setCommitted(true);
            logger.logDebug("Committing op: " + Op.opToString(currOp) + " at index: " + index);

            for (int i = 0; i < log.size(); i++) {
                if (log.get(i).isCommitted()) {
                    if (i > commitIndex) {
                        logger.logDebug("Advanced commit index from " + commitIndex + " to: " + i);
                        commitIndex = i;
                    }
                }
            }
            //checkLocalInvariants();
        } else {
            // error, should not happen
            printState();
            logger.logError("Tried to commit an op that does not exist. Op: " + Op.opToString(op));
        }

    }

    public boolean hasUnreplicated() {
        return !log.isEmpty() && replicateIndex < log.size() - 1;
    }

    public Op getNextUnreplicatedOp() {
        replicateIndex++;
        logger.logDebug("Increment replicateIndex. replicateIndex: " + replicateIndex
                + " commitIndex: " + commitIndex
                + " appliedIndex: " + appliedIndex);
        return log.get(replicateIndex);
    }

    public boolean hasUnappliedOps() {
        return commitIndex > appliedIndex;
    }

    public Op getNextUnappliedOp() {
        appliedIndex++;

        logger.logDebug("Increment appliedIndex. replicateIndex: " + replicateIndex
                + " commitIndex: " + commitIndex
                + " appliedIndex: " + appliedIndex);
        return log.get(appliedIndex);
    }

    public List<Op> truncateUncomittedOps() {
        logger.logDebug("Truncating log to committed index: " + commitIndex);
        List<Op> newLog = new ArrayList<>();
        List<Op> uncommitted = new ArrayList<>();

        for(int i=0; i<log.size(); i++) {
            if (i <= commitIndex) {
                newLog.add(log.get(i));
            } else {
                uncommitted.add(log.get(i));
            }
        }

        uncommitted.addAll(tempLog);

        log = newLog;
        replicateIndex = commitIndex;
        resetIdSourceToLastCommit();
        discardTempLog();
        //checkLocalInvariants();

        return uncommitted;
    }

    private void resetIdSourceToLastCommit() {
        long newIdSource = -1;
        if (commitIndex > -1) {
            newIdSource = log.get(commitIndex).getOpId();
        }

        logger.logDebug("Setting idSource from: " + idSource + " to: " + newIdSource
                + " commitIndex: " + commitIndex + " log size: " + log.size());
        idSource = newIdSource;
    }

    private void checkLocalInvariants() {
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
