package com.raft.server.rpc;

import java.io.Serializable;

public class AppendEntriesResponse implements Serializable {
    long term;
    boolean success;

    public AppendEntriesResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public long getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }
}
