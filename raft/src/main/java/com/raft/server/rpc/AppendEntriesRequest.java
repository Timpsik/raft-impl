package com.raft.server.rpc;

import com.raft.server.entries.LogEntry;

public final class AppendEntriesRequest extends ServerRequest {

    private int leaderId;
    private long prevLogTerm;
    private int prevLogIndex;
    private int leaderCommit;
    private LogEntry[] entries;

    public AppendEntriesRequest(long term, int leaderId, long prevLogTerm, int prevLogIndex, int leaderCommit, LogEntry[] entries) {
        super(term);
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
