package com.raft.requests;

import java.io.Serializable;

public class ChangeStateResponse implements Serializable {
    private int leaderId;
    private boolean success;

    public ChangeStateResponse(int leaderId, boolean success) {
        this.leaderId = leaderId;
        this.success = success;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public boolean isSuccess() {
        return success;
    }
}
