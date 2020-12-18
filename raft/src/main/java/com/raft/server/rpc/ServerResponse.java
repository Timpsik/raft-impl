package com.raft.server.rpc;

import java.io.Serializable;

/**
 * Generic parent to all server-to-server communication responses
 */
public abstract class ServerResponse implements Serializable {
    long term;

    public ServerResponse(long term) {
        this.term = term;
    }

    public long getTerm() {
        return term;
    }
}
