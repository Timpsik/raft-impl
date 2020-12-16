package com.raft.server.rpc;

public class InstallSnapshotResponse extends ServerResponse {
    public InstallSnapshotResponse(long term) {
        this.term = term;

    }
}
