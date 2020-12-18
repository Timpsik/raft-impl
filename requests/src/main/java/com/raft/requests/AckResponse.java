package com.raft.requests;

/**
 * Generic repsonse returned by server.
 */
public class AckResponse extends RaftResponse {

    public AckResponse(String leaderAddress, boolean success) {
        super(success, leaderAddress);
    }

    public AckResponse(String leaderAddress, boolean success, ErrorCause cause) {
        super(success, leaderAddress, cause);
    }

}
