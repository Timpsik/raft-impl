package com.raft.server.rpc;

import java.io.Serializable;

public class RequestVoteResponse implements Serializable {
    private long term;
    private boolean voteGranted;

    public RequestVoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public long getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }
}
