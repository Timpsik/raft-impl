package com.raft.requests;

import java.io.Serializable;

public abstract class RaftRequest implements Serializable {
    final int clientId;
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
