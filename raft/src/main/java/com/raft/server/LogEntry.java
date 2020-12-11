package com.raft.server;

final public class LogEntry {
    private final long term;
    private final StateChange change;

    public LogEntry(long term, StateChange change) {
        this.term = term;
        this.change = change;
    }

    public long getTerm() {
        return term;
    }

    public StateChange getChange() {
        return change;
    }
}
