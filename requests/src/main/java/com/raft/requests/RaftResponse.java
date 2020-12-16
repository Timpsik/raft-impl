package com.raft.requests;

import java.io.Serializable;

public interface RaftResponse extends Serializable {

    boolean isSuccess();

    String getLeaderAddress();
}
