package com.raft.requests;

import java.io.Serializable;

public class ChangeStateRequest implements Serializable {
    private String var;
    private int value;

    public ChangeStateRequest(String var, int value) {
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
