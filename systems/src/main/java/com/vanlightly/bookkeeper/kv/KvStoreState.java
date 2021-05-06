package com.vanlightly.bookkeeper.kv;

import com.vanlightly.bookkeeper.util.LogManager;
import com.vanlightly.bookkeeper.util.Logger;
import com.vanlightly.bookkeeper.kv.log.Position;

import java.util.concurrent.atomic.AtomicInteger;

public class KvStoreState {

    public enum Role {
        NONE,
        FOLLOWER,
        LEADER
    }

    public enum FollowerState {
        NONE,
        NEED_READER,
        READING
    }

    public enum LeaderState {
        NONE,
        START_SEQ_1,
        START_SEQ_2_CACHE_LEDGER_LIST,
        START_SEQ_3_CLOSE_SEGMENT,
        START_SEQ_4_SEGMENT_CLOSING,
        START_SEQ_5_NEED_CATCHUP_READER,
        START_SEQ_6_CATCHUP_READING,
        START_SEQ_7_NEED_WRITER,
        START_SEQ_8_STARTING_WRITER,
        CLOSING_WRITER,
        READY
    }

    private Logger logger = LogManager.getLogger(this.getClass().getSimpleName());
    private AtomicInteger stateCounter;

    public Role role;
    public LeaderState leaderState;
    public FollowerState followerState;

    public KvStoreState() {
        role = Role.NONE;
        leaderState = LeaderState.NONE;
        followerState = FollowerState.NONE;
        stateCounter = new AtomicInteger(0);
    }

    public int getStateCtr() {
        return stateCounter.get();
    }

    public boolean isInState(int stateCtr) {
        return stateCounter.get() == stateCtr;
    }

    public int changeRole(Role r) {
        logger.logInfo("Role change. From: " + role + " to: " + r);
        role = r;
        return stateCounter.incrementAndGet();
    }

    public int changeLeaderState(KvStoreState.LeaderState lState, Position cursor) {
        logger.logInfo("Leader state change. From: " + leaderState + " to: " + lState
                + " (cursor: " + cursor + ")");
        leaderState = lState;
        return stateCounter.incrementAndGet();
    }

    public int changeFollowerState(KvStoreState.FollowerState fState, Position cursor) {
        logger.logInfo("Follower state change. From: " + this.followerState + " to: " + fState
                + " (cursor: " + cursor + ")");
        followerState = fState;
        return stateCounter.incrementAndGet();
    }
}
