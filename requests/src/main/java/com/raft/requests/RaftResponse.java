package com.raft.requests;

public interface RaftResponse  {

    boolean isSuccess();

    String getLeaderAddress();
}
