package com.raft.requests;

import java.io.Serializable;

/**
 * Parent to all requests sent to Raft cluster
 */
public abstract class RaftRequest implements Serializable {

    /**
     * Client id of the request author
     */
    final int clientId;

    /**
     * Number of the request for current client
     */
    final long requestNr;

    public RaftRequest(int clientId, long requestNr) {
        this.clientId = clientId;
        this.requestNr = requestNr;
    }


    public int getClientId() {
        return clientId;
    }

    public long getRequestNr() {
        return requestNr;
    }
}
