package com.raft.server.rpc;

/**
 * Response to the {@link InstallSnapshotRequest}
 */
public class InstallSnapshotResponse extends ServerResponse {
    public InstallSnapshotResponse(long term) {
        super(term);

    }
}
