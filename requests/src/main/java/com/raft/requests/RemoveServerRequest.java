package com.raft.requests;

/**
 * Request to remove server from the cluster configuration
 */
public class RemoveServerRequest extends RaftRequest {

    /**
     * Id of the server that is going to be removed
     */
    private final int removedServerId;

    public RemoveServerRequest(int newServerId, int clientId, long requestNr) {
        super(clientId, requestNr);
        this.removedServerId = newServerId;
    }

    public int getRemovedServerId() {
        return removedServerId;
    }
}
