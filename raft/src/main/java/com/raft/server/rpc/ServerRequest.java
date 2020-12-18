package com.raft.server.rpc;

import java.io.Serializable;

/**
 * Generic parent to all server-to-server communication requests
 */
public abstract class ServerRequest implements Serializable {

    /**
     * Term of the request
     */
    long term;

    /**
     * Id of the sender
     */
    int senderId;

    public ServerRequest(long term, int senderId) {
        this.term = term;
        this.senderId = senderId;
    }

    public long getTerm() {
        return term;
    }

    public int getSenderId() {
        return senderId;
    }
}
