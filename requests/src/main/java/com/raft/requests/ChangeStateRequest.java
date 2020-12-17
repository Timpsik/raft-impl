package com.raft.requests;

import java.io.Serializable;

public class ChangeStateRequest extends RaftRequest {
    private String var;
    private int value;

    public ChangeStateRequest(String var, int value, int clientId, long requestNr) {
        super(clientId, requestNr);
        this.var = var;
        this.value = value;
    }

    public String getVar() {
        return var;
    }

    public int getValue() {
        return value;
    }
}
