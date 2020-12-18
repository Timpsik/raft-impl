package com.raft.requests;

/**
 * Store a variable in the Raft cluster
 */
public class ChangeStateRequest extends RaftRequest {

    /**
     * Name of the variable
     */
    private String var;

    /**
     * Integer value of the variable
     */
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
