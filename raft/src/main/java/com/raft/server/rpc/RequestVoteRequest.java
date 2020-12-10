package com.raft.server.rpc;

import java.io.Serializable;

public class RequestVoteRequest implements Serializable {
    long term;
    int candidateId;
    long lastLogIndex;
    long lastLogTerm;

    public RequestVoteRequest(long term, int candidateId, long lastLogIndex, long lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public long getTerm() {
        return term;
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
