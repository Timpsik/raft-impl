package com.raft.requests;

public class AckResponse extends RaftResponse {

    public AckResponse(String leaderAddress, boolean success) {
        super(success, leaderAddress);
    }

}
