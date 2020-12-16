package com.raft.requests;

public class RemoveServerResponse implements RaftResponse {
    private final boolean success;
    private final String leaderAddress;

    public RemoveServerResponse(boolean success, String leaderAddress) {
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
