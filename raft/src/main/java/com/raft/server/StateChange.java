package com.raft.server;

import java.io.Serializable;

final public class StateChange implements Serializable {
    String key;
    String value;

    public StateChange(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
