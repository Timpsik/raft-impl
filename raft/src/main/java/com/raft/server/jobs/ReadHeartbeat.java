package com.raft.server.jobs;

import com.raft.server.Configuration;
import com.raft.server.LogEntry;
import com.raft.server.RaftServer;
import com.raft.server.ServerState;
import com.raft.server.rpc.AppendEntriesRequest;
import com.raft.server.rpc.AppendEntriesResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ReadHeartbeat implements Runnable {
    private static Logger logger = LogManager.getLogger(ReadHeartbeat.class);

    private final RaftServer raftServer;
    private final String address;
    private final int serverId;
    private final CountDownLatch countDownLatch;
    private final AtomicInteger successCounter;

    public ReadHeartbeat(RaftServer raftServer, String address, int serverId, CountDownLatch countDownLatch, AtomicInteger successCounter) {
        this.raftServer = raftServer;
        this.address = address;
        this.serverId = serverId;
        this.countDownLatch = countDownLatch;
        this.successCounter = successCounter;
    }


    @Override
    public void run() {
        if (raftServer.getState() == ServerState.LEADER) {
            AppendEntriesRequest r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(),
                    raftServer.getLastLogEntryTerm(), raftServer.getLogEntries().size() - 1, raftServer.getCommitIndex().get(), new LogEntry[0]);
            logger.info("Sending log entries request to " + address);
            AppendEntriesResponse response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
            if (response == null) {
                logger.warn("Error when sending append logs to " + address);
                new Thread(new AppendLogEntries(raftServer, address, serverId, countDownLatch, successCounter)).start();
                countDownLatch.countDown();
                return;
            }
            if (response.getTerm() != raftServer.getCurrentTerm().get()) {
                countDownLatch.countDown();
                return;
            }
            if (response.isSuccess()) {
                successCounter.incrementAndGet();
            } else {
                logger.warn("Heartbeat failed to " + address);
                if (response.getTerm() > raftServer.getCurrentTerm().get()) {
                    raftServer.setState(ServerState.FOLLOWER);
                    return;
                } else {
                    int nextIndex = -1;
                    LogEntry[] entries = null;
                    while (!response.isSuccess()) {
                        if (raftServer.getState() == ServerState.LEADER) {
                            nextIndex = raftServer.reduceAndGetNextIndex(serverId);
                            entries = raftServer.getLogEntriesSince(nextIndex);
                            logger.warn("Fixing followers logˇ, next Index is:  " + nextIndex + ", sending " + entries.length + " entries");
                            r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(),
                                    raftServer.getLogEntryTerm(nextIndex - 1), nextIndex - 1, raftServer.getCommitIndex().get(), entries);

                            response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
                            if (response == null) {
                                raftServer.getScheduler().schedule(new Heartbeat(raftServer, address, serverId), Configuration.minElectionTimeout / 2, TimeUnit.MILLISECONDS);
                                countDownLatch.countDown();
                                return;
                            }
                            if (response.getTerm() != raftServer.getCurrentTerm().get()) {
                                countDownLatch.countDown();
                                return;
                            }
                            if (response.getTerm() > raftServer.getCurrentTerm().get()) {
                                logger.warn("Log append failed to " + address + ", not leader anymore");
                                raftServer.setState(ServerState.FOLLOWER);
                                countDownLatch.countDown();
                                return;
                            }
                        }
                    }
                    if (entries != null) {
                        raftServer.setNextIndex(serverId, nextIndex + entries.length);
                        raftServer.setMatchIndex(serverId, nextIndex + entries.length - 1);
                        successCounter.incrementAndGet();
                    }

                }
            }
            countDownLatch.countDown();
        }
    }
}
