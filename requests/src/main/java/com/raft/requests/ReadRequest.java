package com.raft.requests;

public class ReadRequest extends RaftRequest {
    private String var;

    public ReadRequest(String var, int clientId, long requestNr) {
        super(clientId, requestNr);
        this.var = var;
    }

    public String getVar() {
        return var;
    }
}
