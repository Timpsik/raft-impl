package com.raft.server.jobs;

import com.raft.server.Configuration;
import com.raft.server.LogEntry;
import com.raft.server.RaftServer;
import com.raft.server.ServerState;
import com.raft.server.rpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

public class Heartbeat implements Runnable {
    private static Logger logger = LogManager.getLogger(Heartbeat.class);

    private final RaftServer raftServer;
    private final String address;
    private final int serverId;

    public Heartbeat(RaftServer raftServer, String address, int serverId) {
        this.raftServer = raftServer;
        this.address = address;
        this.serverId = serverId;
    }

    @Override
    public void run() {
        if (raftServer.getState() == ServerState.LEADER) {
            AppendEntriesRequest r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(),
                    raftServer.getLastLogEntryTerm(), raftServer.getLogEntries().size() - 1, raftServer.getCommitIndex().get(), new LogEntry[0]);
            //logger.debug("Sending heartbeat request to " + address);
            AppendEntriesResponse response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
            if (response == null) {
                raftServer.getScheduler().schedule(new Heartbeat(raftServer, address, serverId), Configuration.minElectionTimeout / 2, TimeUnit.MILLISECONDS);
                return;
            }
            if (response.getTerm() != raftServer.getCurrentTerm().get()) {
                return;
            }
            if (response.isSuccess()) {
                //logger.debug("Successful heartbeat sent to " + address);
            } else {
                logger.warn("Heartbeat failed to " + address);
                if (response.getTerm() > raftServer.getCurrentTerm().get()) {
                    raftServer.setState(ServerState.FOLLOWER);
                    return;
                } else {

                    while (!response.isSuccess()) {
                        if (raftServer.getState() == ServerState.LEADER) {
                            int nextIndex = raftServer.reduceAndGetNextIndex(serverId);
                            logger.warn("Fixing followers logˇ, next Index is:  " + nextIndex);
                            LogEntry[] entries = raftServer.getLogEntriesSince(nextIndex);
                            r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(),
                                    raftServer.getLogEntryTerm(nextIndex - 1), nextIndex - 1, raftServer.getCommitIndex().get(), entries);

                            response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
                            if (response == null) {
                                raftServer.getScheduler().schedule(new Heartbeat(raftServer, address, serverId), Configuration.minElectionTimeout / 2, TimeUnit.MILLISECONDS);
                                return;
                            }
                            if (response.getTerm() != raftServer.getCurrentTerm().get()) {
                                return;
                            }
                            if (response.getTerm() > raftServer.getCurrentTerm().get()) {
                                logger.warn("Log append failed to " + address + ", not leader anymore");
                                raftServer.setState(ServerState.FOLLOWER);
                                return;
                            }
                        }
                    }

                }
            }
            raftServer.getScheduler().schedule(new Heartbeat(raftServer, address, serverId), Configuration.minElectionTimeout / 2, TimeUnit.MILLISECONDS);
        }
    }
}
