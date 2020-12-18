package com.raft.requests;

/**
 * Make request to the server to add new server into the cluster
 */
public class AddServerRequest extends RaftRequest {

    /**
     * Address of the new server
     */
    private final String newServerAddress;

    /**
     * Id of the new server
     */
    private final int newServerId;

    public AddServerRequest(String newServerAddress, int newServerId, int clientId, long requestNr) {
        super(clientId, requestNr);
        this.newServerAddress = newServerAddress;
        this.newServerId = newServerId;
    }

    public String getNewServerAddress() {
        return newServerAddress;
    }

    public int getNewServerId() {
        return newServerId;
    }
}
