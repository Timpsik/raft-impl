package com.raft.server.rpc;

import com.raft.server.snapshot.Snapshot;

/**
 * Request containing the snapshot for the follower to update itself
 */
public class InstallSnapshotRequest extends ServerRequest {


    /**
     * Actual snapshot containing the data
     */
    private Snapshot snapshot;

    public InstallSnapshotRequest(long term, int leaderId, Snapshot snapshot) {
        super(term, leaderId);
        this.snapshot = snapshot;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

}
