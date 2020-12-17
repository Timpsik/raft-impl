package com.raft.requests;

import java.io.Serializable;

public abstract class RaftResponse implements Serializable {

    boolean success;
    String leaderAddress;
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
