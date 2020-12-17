package com.raft.server.rpc;

public class RequestVoteRequest extends ServerRequest {

    private int candidateId;
    private long lastLogIndex;
    private long lastLogTerm;

    public RequestVoteRequest(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
        super(term);
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }
}
