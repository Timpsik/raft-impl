package com.raft.server.rpc;

import com.raft.server.LogEntry;

import java.io.Serializable;

public final class AppendEntriesRequest implements Serializable {
    long term;
    int leaderId;
    long prevLogTerm;
    long prevLogIndex;
    long leaderCommit;
    LogEntry[] entries;

    public AppendEntriesRequest(long term, int leaderId, long prevLogTerm, long prevLogIndex, long leaderCommit, LogEntry[] entries) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }

    public long getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public LogEntry[] getEntries() {
        return entries;
    }
}
