package com.raft.server.rpc;

import com.raft.server.snapshot.Snapshot;

public class InstallSnapshotRequest extends ServerRequest {
    private int leaderId;
    private Snapshot snapshot;
    private long term;

    public InstallSnapshotRequest(long term, int leaderId, Snapshot snapshot) {
        this.leaderId = leaderId;
        this.snapshot = snapshot;
        this.term = term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    @Override
    public long getTerm() {
        return term;
    }
}
