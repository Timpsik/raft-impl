package com.raft.server.entries;

import java.io.Serializable;

public final class LogEntry implements Serializable {
    private final long term;
    private final Change change;
    private final int clientId;
    private final long requestNr;
    private final int index;

    public LogEntry(long term, Change change, int clientId, long requestNr, int index) {
        this.term = term;
        this.change = change;
        this.clientId = clientId;
        this.requestNr = requestNr;
        this.index = index;
    }

    public long getTerm() {
        return term;
    }

    public Change getChange() {
        return change;
    }

     public int getClientId() {
         return clientId;
     }

     public long getRequestNr() {
         return requestNr;
     }

    public int getIndex() {
        return index;
    }
}
