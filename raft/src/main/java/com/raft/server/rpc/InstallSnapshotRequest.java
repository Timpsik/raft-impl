package com.raft.server.rpc;

import com.raft.server.snapshot.Snapshot;

public class InstallSnapshotRequest extends ServerRequest {
    private int leaderId;
    private Snapshot snapshot;
    private long term;

    public InstallSnapshotRequest(long term, int leaderId, Snapshot snapshot) {
        super(term);
        this.leaderId = leaderId;
        this.snapshot = snapshot;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

}
