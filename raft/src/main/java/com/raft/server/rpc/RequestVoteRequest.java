package com.raft.server.rpc;

/**
 * Request for asking for vote from another server in the cluster
 */
public class RequestVoteRequest extends ServerRequest {

    /**
     * Last index in the sender log
     */
    private long lastLogIndex;

    /**
     * Term of the last index
     */
    private long lastLogTerm;

    public RequestVoteRequest(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
        super(term, candidateId);
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }
}
