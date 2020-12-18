package com.raft.server.snapshot;

import java.io.Serializable;
import java.util.Map;

/**
 * Snapshot of the current system state
 */
public final class Snapshot implements Serializable {
    /**
     * Last log index included in the snapshot
     */
    private int lastLogIndex;

    /**
     * Term of the last included log index
     */
    private long lastTerm;

    /**
     * Current storage of the server
     */
    private Map<String, Integer> storage;

    /**
     * Server id-s and server addresses
     */
    private Map<Integer, String> serversConfiguration;

    /**
     * Client id-s and the last served request.
     */
    private Map<Integer, Long> servedClients;

    public Snapshot(int lastLogIndex, long lastTerm, Map<String, Integer> storage, Map<Integer, String> serversConfiguration, Map<Integer, Long> servedClients) {
        this.lastLogIndex = lastLogIndex;
        this.lastTerm = lastTerm;
        this.storage = storage;
        this.serversConfiguration = serversConfiguration;
        this.servedClients = servedClients;
    }


    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastTerm() {
        return lastTerm;
    }

    public Map<String, Integer> getStorage() {
        return storage;
    }

    public Map<Integer, String> getServersConfiguration() {
        return serversConfiguration;
    }

    public Map<Integer, Long> getServedClients() {
        return servedClients;
    }
}
