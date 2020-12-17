package com.raft.requests;

public enum ErrorCause {
    ALREADY_PROCESSED,
    ENTRY_NOT_FOUND,
    NOT_LEADER
}
