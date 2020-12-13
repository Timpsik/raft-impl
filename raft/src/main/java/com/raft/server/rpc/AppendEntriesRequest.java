package com.raft.server.rpc;

import com.raft.server.LogEntry;

import java.io.Serializable;

public final class AppendEntriesRequest extends ServerRequest {

    int leaderId;
    long prevLogTerm;
    int prevLogIndex;
    int leaderCommit;
    LogEntry[] entries;

    public AppendEntriesRequest(long term, int leaderId, long prevLogTerm, int prevLogIndex, int leaderCommit, LogEntry[] entries) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }



    public int getLeaderId() {
        return leaderId;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public LogEntry[] getEntries() {
        return entries;
    }
}
