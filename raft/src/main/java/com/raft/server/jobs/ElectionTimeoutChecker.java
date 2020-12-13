package com.raft.server.jobs;

import com.raft.server.RaftServer;
import com.raft.server.ServerState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class ElectionTimeoutChecker implements Runnable {
    private static Logger logger = LogManager.getLogger(ElectionTimeoutChecker.class);

    private RaftServer raftServer;
    public ElectionTimeoutChecker(RaftServer raftServer) {
        this.raftServer = raftServer;
    }

    @Override
    public void run() {
        long nanoTime = System.nanoTime();
        if (raftServer.getState() != ServerState.LEADER && raftServer.getLastHeardFromLeader() + raftServer.getElectionTimeout() * 1000 * 1000 < nanoTime) {
            if (logger.isDebugEnabled()) {
                logger.debug("Starting election, currentTime : " + nanoTime + " Last heard time: " + raftServer.getLastHeardFromLeader() + raftServer.getElectionTimeout() * 1000 * 1000);
            }

            raftServer.convertToCandidate();
            raftServer.sendVoteRequests();
            raftServer.getScheduler().schedule(new ElectionTimeoutChecker(raftServer), raftServer.generateNewElectionTimeout(), TimeUnit.MILLISECONDS);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Setting new election timeout");
            }
            raftServer.getScheduler().schedule(new ElectionTimeoutChecker(raftServer), raftServer.generateNewElectionTimeout(), TimeUnit.MILLISECONDS);
        }
        raftServer.resetVotedFor();
        raftServer.resetVotes();
    }
}
