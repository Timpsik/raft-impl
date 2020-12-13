package com.raft.server.rpc;

import java.io.Serializable;

public class RequestVoteResponse extends ServerResponse {

    private boolean voteGranted;

    public RequestVoteResponse(long term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }


    public boolean isVoteGranted() {
        return voteGranted;
    }
}
