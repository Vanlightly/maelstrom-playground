package com.vanlightly.bookkeeper.kv;

import com.vanlightly.bookkeeper.Logger;

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
        NEED_CLOSE_SEGMENT,
        CLOSING_SEGMENT,
        NEED_CATCHUP_READER,
        CATCHUP_READING,
        NEED_WRITER,
        STARTING_WRITER,
        CLOSING_WRITER,
        READY
    }

    private Logger logger;
    private AtomicInteger stateCounter;

    public Role role;
    public LeaderState leaderState;
    public FollowerState followerState;

    public KvStoreState(Logger logger) {
        this.logger = logger;
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

    public int changeLeaderState(KvStoreState.LeaderState lState) {
        logger.logInfo("Leader state change. From: " + leaderState + " to: " + lState);
        leaderState = lState;
        return stateCounter.incrementAndGet();
    }

    public int changeFollowerState(KvStoreState.FollowerState fState) {
        logger.logInfo("Follower state change. From: " + this.followerState + " to: " + fState);
        followerState = fState;
        return stateCounter.incrementAndGet();
    }
}
