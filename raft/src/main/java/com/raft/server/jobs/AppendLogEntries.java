package com.raft.server.jobs;

import com.raft.server.LogEntry;
import com.raft.server.RaftServer;
import com.raft.server.ServerState;
import com.raft.server.rpc.AppendEntriesRequest;
import com.raft.server.rpc.AppendEntriesResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class AppendLogEntries implements Runnable {


    private static Logger logger = LogManager.getLogger(AppendLogEntries.class);

    private final RaftServer raftServer;
    private final String address;
    private final int serverId;
    private CountDownLatch countDownLatch;
    private AtomicInteger successCounter;

    public AppendLogEntries(RaftServer raftServer, String address, int serverId, CountDownLatch countDownLatch, AtomicInteger successCounter) {
        this.raftServer = raftServer;
        this.address = address;
        this.serverId = serverId;
        this.countDownLatch = countDownLatch;
        this.successCounter = successCounter;
    }

    @Override
    public void run() {
        if (raftServer.getState() == ServerState.LEADER) {
            int nextIndex = raftServer.getNextIndex(serverId);
            LogEntry[] entries = raftServer.getLogEntriesSince(nextIndex);
            AppendEntriesRequest r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(),
                    raftServer.getLogEntryTerm(nextIndex - 1), nextIndex - 1, raftServer.getCommitIndex().get(), entries);
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
                logger.info("Successful log append sent to " + address);
                raftServer.setNextIndex(serverId, nextIndex + entries.length - 1);
                raftServer.setMatchIndex(serverId, nextIndex + entries.length - 1);
                successCounter.incrementAndGet();
            } else {
                if (response.getTerm() <= raftServer.getCurrentTerm().get()) {
                    logger.info("log append failed to " + address);
                    while (!response.isSuccess()) {
                        if (raftServer.getState() == ServerState.LEADER) {
                            nextIndex = raftServer.reduceAndGetNextIndex(serverId);
                            entries = raftServer.getLogEntriesSince(nextIndex);
                            r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(),
                                    raftServer.getLogEntryTerm(nextIndex - 1), nextIndex - 1, raftServer.getCommitIndex().get(), entries);
                            response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
                            if (response.getTerm() != raftServer.getCurrentTerm().get()) {
                                countDownLatch.countDown();
                                return;
                            }
                            if (response.getTerm() > raftServer.getCurrentTerm().get()) {
                                logger.warn("Log append failed to " + address + ", not leader anymore");
                                raftServer.setState(ServerState.FOLLOWER);
                                break;
                            }
                        }
                    }
                    if (response.isSuccess()) {
                        raftServer.setNextIndex(serverId, nextIndex + entries.length - 1);
                        raftServer.setMatchIndex(serverId, nextIndex + entries.length - 1);
                        successCounter.incrementAndGet();
                    }
                } else {
                    logger.info("Not leader anymore");
                    raftServer.setState(ServerState.FOLLOWER);
                }
            }
            countDownLatch.countDown();
        }
    }
}
