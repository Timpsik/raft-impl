package com.raft.requests;

/**
 * Read variable from Raft storage
 */
public class ReadRequest extends RaftRequest {

    /**
     * Variable name to read from storage
     */
    private String var;

    public ReadRequest(String var, int clientId, long requestNr) {
        super(clientId, requestNr);
        this.var = var;
    }

    public String getVar() {
        return var;
    }
}
