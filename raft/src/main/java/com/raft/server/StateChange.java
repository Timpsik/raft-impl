package com.raft.server;

final public class StateChange {
    String key;
    String value;

    public StateChange(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
