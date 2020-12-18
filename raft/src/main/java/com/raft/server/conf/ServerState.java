package com.raft.server.conf;

/**
 * The possible states of server
 */
public enum ServerState {
    LEADER, // Server is leader, sends heartbeats to others and serves clients
    CANDIDATE, // Server tries to become the new leader
    FOLLOWER // Server is follower and listens to updates from the leader
}
