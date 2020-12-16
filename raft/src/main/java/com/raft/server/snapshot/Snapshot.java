package com.raft.server.snapshot;

import java.io.Serializable;
import java.util.Map;

public final class Snapshot implements Serializable {
    private int lastLogIndex;
    private long lastTerm;
    private Map<String,Integer> state;
    private Map<Integer, String> serversConfiguration;

    public Snapshot(int lastLogIndex, long lastTerm, Map<String, Integer> state, Map<Integer, String> serversConfiguration) {
        this.lastLogIndex = lastLogIndex;
        this.lastTerm = lastTerm;
        this.state = state;
        this.serversConfiguration = serversConfiguration;
    }


    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastTerm() {
        return lastTerm;
    }

    public Map<String, Integer> getState() {
        return state;
    }

    public Map<Integer, String> getServersConfiguration() {
        return serversConfiguration;
    }
}
