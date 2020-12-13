package com.raft.requests;

import java.io.Serializable;

public class ReadResponse implements Serializable, RaftResponse {

    private String leaderAddress;
    private boolean success;
    private int value;


    public ReadResponse(String leaderAddress, boolean success, int value) {
        this.leaderAddress = leaderAddress;
        this.success = success;
        this.value = value;
    }

    @Override
    public boolean isSuccess() {
        return success;
    }

    @Override
    public String getLeaderAddress() {
        return leaderAddress;
    }

    public int getValue() {
        return value;
    }
}
