package com.raft.server;

public enum ServerState {
    LEADER,
    CANDIDATE,
    FOLLOWER,
    CATCHUP
}
