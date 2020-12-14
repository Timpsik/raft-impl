package com.raft.server;

import java.io.Serializable;

public final class LogEntry implements Serializable {
    private final long term;
    private final StateChange change;
    private final int clientId;
    private final long requestNr;

    public LogEntry(long term, StateChange change, int clientId, long requestNr) {
        this.term = term;
        this.change = change;
        this.clientId = clientId;
        this.requestNr = requestNr;
    }

    public long getTerm() {
        return term;
    }

    public StateChange getChange() {
        return change;
    }

     public int getClientId() {
         return clientId;
     }

     public long getRequestNr() {
         return requestNr;
     }
 }
