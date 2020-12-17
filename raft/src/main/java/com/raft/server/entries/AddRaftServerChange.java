package com.raft.server.entries;

public class AddRaftServerChange extends Change {

    private int serverId;
    private String serverAddress;

    public AddRaftServerChange(int serverId, String serverAddress) {
        this.serverId = serverId;
        this.serverAddress = serverAddress;
    }

    public int getServerId() {
        return serverId;
    }

    public String getServerAddress() {
        return serverAddress;
    }
}
