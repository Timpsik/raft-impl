package com.raft.server.jobs;

import com.raft.server.RaftServer;
import com.raft.server.ServerState;
import com.raft.server.rpc.RequestVoteRequest;
import com.raft.server.rpc.RequestVoteResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class RequestVote implements Runnable {
    private static Logger logger = LogManager.getLogger(RequestVote.class);


    private final RaftServer raftServer;
    private final String address;
    private final int serverId;

    public RequestVote(RaftServer raftServer, String address, int serverId) {
        this.raftServer = raftServer;
        this.address = address;
        this.serverId = serverId;
    }

    @Override
    public void run() {
        RequestVoteRequest r = new
                RequestVoteRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(), raftServer.getLastApplied().get(), raftServer.getLastLogEntryTerm());
        logger.info("Sending vote request to " + address + " for term " + raftServer.getCurrentTerm().get());

            RequestVoteResponse response = (RequestVoteResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
            try {
                raftServer.getElectionLock();
                if (response == null || r.getTerm() != raftServer.getCurrentTerm().get()) {
                    return;
                }
            } finally {
                raftServer.releaseElectionLock();
            }

            if (response.isVoteGranted()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Received vote from " + address + " for term " + r.getTerm());
                }
                logger.info("Received vote from " + address + " for term " + r.getTerm());
                raftServer.addVoteAndCheckWin();
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Did not get vote from " + address);
                }
                logger.info("Did not get vote from " + address + " for term " + r.getTerm());
                if (response.getTerm() > raftServer.getCurrentTerm().get()) {
                    raftServer.setState(ServerState.FOLLOWER);
                }
            }
    }
}
