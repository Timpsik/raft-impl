package com.raft.server;

final public class LogEntry {
    private final int term;
    private final StateChange change;

    public LogEntry(int term, StateChange change) {
        this.term = term;
        this.change = change;
    }

    public int getTerm() {
        return term;
    }

    public StateChange getChange() {
        return change;
    }
}
