package com.raft.server.rpc;

public class RequestVoteResponse extends ServerResponse {

    private boolean voteGranted;

    public RequestVoteResponse(long term, boolean voteGranted) {
        super(term);
        this.voteGranted = voteGranted;
    }


    public boolean isVoteGranted() {
        return voteGranted;
    }
}
