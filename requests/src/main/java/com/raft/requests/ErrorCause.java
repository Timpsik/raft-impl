package com.raft.requests;

/**
 * Possible error causes returned by server
 */
public enum ErrorCause {
    ALREADY_PROCESSED, //Request already processed by server
    ENTRY_NOT_FOUND, // Variable not found in storage, returned to read requests
    NOT_LEADER, // Connected server is not the leader
    REQUEST_FAILED
}
