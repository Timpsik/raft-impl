package com.raft.server.rpc;

import java.io.Serializable;

public class AppendEntriesResponse extends ServerResponse {
    boolean success;

    public AppendEntriesResponse(long term, boolean success) {
        this.term = term;
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }
}
