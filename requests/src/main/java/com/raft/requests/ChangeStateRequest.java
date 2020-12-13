package com.raft.requests;

import java.io.Serializable;

public class ChangeStateRequest implements Serializable,RaftRequest {
    private String var;
    private int value;
    private int clientId;
    private long requestNr;

    public ChangeStateRequest(String var, int value, int clientId, long requestNr) {
        this.var = var;
        this.value = value;
        this.clientId = clientId;
        this.requestNr = requestNr;
    }

    public String getVar() {
        return var;
    }

    public int getValue() {
        return value;
    }

    public int getClientId() {
        return clientId;
    }

    public long getRequestNr() {
        return requestNr;
    }
}
