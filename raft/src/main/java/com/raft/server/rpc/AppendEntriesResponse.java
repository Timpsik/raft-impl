package com.raft.server.rpc;

/**
 * Response to {@link AppendEntriesRequest}
 */
public class AppendEntriesResponse extends ServerResponse {

    /**
     * True if the append entries request was successful
     */
    boolean success;

    public AppendEntriesResponse(long term, boolean success) {
        super(term);
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }
}
