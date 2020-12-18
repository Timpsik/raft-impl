package com.raft.server.rpc;

import com.raft.server.entries.LogEntry;

/**
 * Request to send the new log entries to the followers. Also used as a heartbeat from leader to followers.
 */
public final class AppendEntriesRequest extends ServerRequest {

    /**
     * Index of the log entry right before the ones in entries field
     */
    private int prevLogIndex;

    /**
     * Term of the log entry at previous log entry index
     */
    private long prevLogTerm;

    /**
     * Commit index of the sender
     */
    private int leaderCommit;

    /**
     * Entries to add to log
     */
    private LogEntry[] entries;

    public AppendEntriesRequest(long term, int leaderId, long prevLogTerm, int prevLogIndex, int leaderCommit, LogEntry[] entries) {
        super(term, leaderId);
        this.prevLogTerm = prevLogTerm;
        this.prevLogIndex = prevLogIndex;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
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
