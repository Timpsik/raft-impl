package com.raft.server.rpc;

public class InstallSnapshotResponse extends ServerResponse {
    public InstallSnapshotResponse(long term) {
        super(term);

    }
}
