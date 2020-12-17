package com.raft.server.rpc;

public class AppendEntriesResponse extends ServerResponse {
    boolean success;

    public AppendEntriesResponse(long term, boolean success) {
        super(term);
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }
}
