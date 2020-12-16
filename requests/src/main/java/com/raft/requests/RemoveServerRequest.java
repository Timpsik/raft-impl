package com.raft.requests;

public class RemoveServerRequest implements RaftRequest {
    private final int removedServerId;
    private final int clientId;
    private final long requestNr;

    public RemoveServerRequest(int newServerId, int clientId, long requestNr) {
        this.removedServerId = newServerId;
        this.clientId = clientId;
        this.requestNr = requestNr;
    }

    public int getRemovedServerId() {
        return removedServerId;
    }

    public int getClientId() {
        return clientId;
    }

    public long getRequestNr() {
        return requestNr;
    }
}
