package com.raft.server;

public class RemoveRaftServerChange extends Change {

    private int serverId;

    public RemoveRaftServerChange(int serverId) {
        this.serverId = serverId;
    }

    public int getServerId() {
        return serverId;
    }

}
