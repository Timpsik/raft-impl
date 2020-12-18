package com.raft.server.jobs;

import com.raft.server.RaftServer;
import com.raft.server.conf.ServerState;
import com.raft.server.rpc.RequestVoteRequest;
import com.raft.server.rpc.RequestVoteResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handles requesting vote from other server
 */
public class RequestVote implements Runnable {
    private static Logger logger = LogManager.getLogger(RequestVote.class);

    private final RaftServer raftServer;

    /**
     * Address of the other server
     */
    private final String address;

    /**
     * Id of the other server
     */
    private final int serverId;

    public RequestVote(RaftServer raftServer, String address, int serverId) {
        this.raftServer = raftServer;
        this.address = address;
        this.serverId = serverId;
    }

    @Override
    public void run() {
        RequestVoteRequest r = new
                RequestVoteRequest(raftServer.getCurrentTerm(), raftServer.getServerId(), raftServer.getLastApplied(), raftServer.getLastLogEntryTerm());
        logger.info("Sending vote request to " + address + " for term " + raftServer.getCurrentTerm());

        RequestVoteResponse response = (RequestVoteResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
        try {
            raftServer.getElectionLock();
            if (response == null || r.getTerm() != raftServer.getCurrentTerm()) {
                return;
            }
        } finally {
            raftServer.releaseElectionLock();
        }

        // Check if got vote
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
            // Other server had bigger term, return to follower state
            if (response.getTerm() > raftServer.getCurrentTerm()) {
                raftServer.setState(ServerState.FOLLOWER);
            }
        }
    }
}
