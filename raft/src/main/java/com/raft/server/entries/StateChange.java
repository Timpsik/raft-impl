package com.raft.server.entries;

/**
 * Store variable in storage
 */
final public class StateChange extends Change {
    /**
     * Variable name
     */
    private String key;

    /**
     * Variable value
     */
    private String value;

    public StateChange(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
