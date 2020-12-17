package com.raft.requests;

public class ReadResponse extends RaftResponse {

    private int value;


    public ReadResponse(String leaderAddress, boolean success, int value) {
        super(success,leaderAddress);
        this.value = value;
    }

    public ReadResponse(String leaderAddress, boolean success, ErrorCause cause) {
        super(success, leaderAddress, cause);
    }

    public int getValue() {
        return value;
    }

}
