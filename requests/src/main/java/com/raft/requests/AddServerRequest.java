package com.raft.requests;

public class AddServerRequest extends RaftRequest {

    private final String newServerAddress;
    private final int newServerId;

    public AddServerRequest(String newServerAddress, int newServerId, int clientId, long requestNr) {
        super(clientId,requestNr);
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
