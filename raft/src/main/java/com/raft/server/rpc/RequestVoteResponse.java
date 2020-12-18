package com.raft.server.rpc;

/**
 * Vote request response
 */
public class RequestVoteResponse extends ServerResponse {

    /**
     * True if the vote was granted to the request sender
     */
    private boolean voteGranted;

    public RequestVoteResponse(long term, boolean voteGranted) {
        super(term);
        this.voteGranted = voteGranted;
    }


    public boolean isVoteGranted() {
        return voteGranted;
    }
}
