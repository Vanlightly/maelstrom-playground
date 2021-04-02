package com.vanlightly.bookkeeper.kv;

public class KvStoreState {

    public enum Role {
        NONE,
        FOLLOWER,
        LEADER
    }

    public enum FollowerState {
        NEED_READER,
        READING
    }

    public enum LeaderState {
        NEED_CLOSE_SEGMENT,
        NEED_CATCHUP_READER,
        STARTING_CATCHUP_READER,
        CATCHUP_READING,
        NEED_WRITER,
        STARTING_WRITER,
        CLOSING_WRITER,
        READY
    }

    public KvStoreState() {
        role = Role.NONE;

    }

    public Role role;
    public LeaderState leaderState;
    public FollowerState followerState;


}
