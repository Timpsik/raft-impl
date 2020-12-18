package com.raft.server.entries;

/**
 * Add new server to the configuration
 */
public class AddRaftServerChange extends Change {

    /**
     * Id of the new server
     */
    private int serverId;

    /**
     * Address of the new server
     */
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
