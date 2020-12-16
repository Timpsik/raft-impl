package com.raft.requests;

public class AddServerResponse implements RaftResponse {
    private final boolean success;
    private final String leaderAddress;

    public AddServerResponse(String leaderAddress, boolean success) {
        this.success = success;
        this.leaderAddress = leaderAddress;
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
