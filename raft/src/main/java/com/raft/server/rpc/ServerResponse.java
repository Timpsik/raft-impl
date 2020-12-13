package com.raft.server.rpc;

import java.io.Serializable;

public abstract class ServerResponse  implements Serializable {
    long term;

    public long getTerm() {
        return term;
    }
}
