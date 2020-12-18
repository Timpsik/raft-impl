package com.raft.requests;

import java.io.Serializable;

/**
 * Parent class for all responses returned by server to client.
 */
public abstract class RaftResponse implements Serializable {

    /**
     * True if request was successful
     */
    boolean success;

    /**
     * The leader address, so that client can resend the request to leader
     */
    String leaderAddress;

    /**
     * Cause of the request failure.
     */
    ErrorCause cause;

    public RaftResponse(boolean success, String leaderAddress, ErrorCause cause) {
        this.success = success;
        this.leaderAddress = leaderAddress;
        this.cause = cause;
    }

    public RaftResponse(boolean success, String leaderAddress) {
        this.success = success;
        this.leaderAddress = leaderAddress;
    }

    public boolean isSuccess(){
        return success;
    }

    public String getLeaderAddress(){
        return leaderAddress;
    }

    public ErrorCause getCause(){
        return cause;
    }
}
