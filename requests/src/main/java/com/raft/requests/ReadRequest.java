package com.raft.requests;

import java.io.Serializable;

public class ReadRequest implements Serializable, RaftRequest {
    private String var;

    public ReadRequest(String var) {
        this.var = var;
    }

    public String getVar() {
        return var;
    }
}
