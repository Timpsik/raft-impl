package com.raft.server.entries;


final public class StateChange extends Change {
    private String key;
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
