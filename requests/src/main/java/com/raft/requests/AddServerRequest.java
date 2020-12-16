package com.raft.requests;

public class AddServerRequest implements RaftRequest {

    private final String newServerAddress;
    private final int newServerId;
    private final int clientId;
    private final long requestNr;

    public AddServerRequest(String newServerAddress, int newServerId, int clientId, long requestNr) {
        this.newServerAddress = newServerAddress;
        this.newServerId = newServerId;
        this.clientId = clientId;
        this.requestNr = requestNr;
    }

    public String getNewServerAddress() {
        return newServerAddress;
    }

    public int getNewServerId() {
        return newServerId;
    }

    public int getClientId() {
        return clientId;
    }

    public long getRequestNr() {
        return requestNr;
    }
}
