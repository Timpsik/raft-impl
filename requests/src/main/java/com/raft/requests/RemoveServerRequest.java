package com.raft.requests;

public class RemoveServerRequest extends RaftRequest {
    private final int removedServerId;

    public RemoveServerRequest(int newServerId, int clientId, long requestNr) {
        super(clientId, requestNr);
        this.removedServerId = newServerId;
    }

    public int getRemovedServerId() {
        return removedServerId;
    }
}
