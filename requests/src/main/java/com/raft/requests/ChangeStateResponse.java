package com.raft.requests;

import java.io.Serializable;

public class ChangeStateResponse implements Serializable, RaftResponse {
    private String leaderAddress;
    private boolean success;

    public ChangeStateResponse(String leaderAddress, boolean success) {
        this.leaderAddress = leaderAddress;
        this.success = success;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    @Override
    public String getLeaderAddress() {
        return leaderAddress;
    }
}
