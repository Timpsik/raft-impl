package com.raft.server.entries;

import java.io.Serializable;

/**
 * Log entry stored in all of the servers
 */
public final class LogEntry implements Serializable {

    /**
     * Term of the index
     */
    private final long term;

    /**
     * Change of the log entry
     */
    private final Change change;

    /**
     * Index of the log entry
     */
    private final int index;

    /**
     * Client id of the original request sender
     */
    private final int clientId;

    /**
     * Request number of the sender
     */
    private final long requestNr;

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
