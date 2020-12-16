package com.raft.server.jobs;

import com.raft.server.LogEntry;
import com.raft.server.RaftServer;
import com.raft.server.ServerState;
import com.raft.server.rpc.AppendEntriesRequest;
import com.raft.server.rpc.AppendEntriesResponse;
import com.raft.server.rpc.InstallSnapshotRequest;
import com.raft.server.rpc.InstallSnapshotResponse;
import com.raft.server.snapshot.Snapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
            if (entries != null) {
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
                if (r.getTerm() != raftServer.getCurrentTerm().get()) {
                    countDownLatch.countDown();
                    return;
                }
                if (response.isSuccess()) {
                    logger.info("Successful log append sent to " + address);
                    raftServer.setNextIndex(serverId, nextIndex + entries.length);
                    raftServer.setMatchIndex(serverId, nextIndex + entries.length - 1);
                    successCounter.incrementAndGet();
                } else {
                    if (response.getTerm() <= raftServer.getCurrentTerm().get()) {
                        logger.info("log append failed to " + address);
                        while (!response.isSuccess()) {
                            if (raftServer.getState() == ServerState.LEADER) {
                                nextIndex = raftServer.reduceAndGetNextIndex(serverId);
                                entries = raftServer.getLogEntriesSince(nextIndex);
                                if (entries != null) {
                                    r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(),
                                            raftServer.getLogEntryTerm(nextIndex - 1), nextIndex - 1, raftServer.getCommitIndex().get(), entries);
                                    response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
                                    if (r.getTerm() != raftServer.getCurrentTerm().get()) {
                                        countDownLatch.countDown();
                                        return;
                                    }
                                    if (response.getTerm() > raftServer.getCurrentTerm().get()) {
                                        logger.warn("Log append failed to " + address + ", not leader anymore");
                                        raftServer.setState(ServerState.FOLLOWER);
                                        break;
                                    }
                                } else {
                                    // Send snapshot
                                    Snapshot lastSnapshot = raftServer.getLastSnapshot();
                                    if (lastSnapshot == null) {
                                        logger.warn("Entries and snapshots not existing, nothing to bring the follower back");
                                    } else {
                                        InstallSnapshotRequest req = new InstallSnapshotRequest(raftServer.getCurrentTerm().get(), raftServer.getLeaderId(), lastSnapshot);
                                        logger.info("Sending snapshot to " + address);
                                        InstallSnapshotResponse resp = (InstallSnapshotResponse) raftServer.getServerConnection(serverId).sendRequestToServer(req);
                                        if (r.getTerm() != raftServer.getCurrentTerm().get()) {
                                            countDownLatch.countDown();
                                            return;
                                        }
                                        if (resp == null) {
                                            logger.warn("Error when sending snapshot to " + address);
                                            new Thread(new AppendLogEntries(raftServer, address, serverId, countDownLatch, successCounter)).start();
                                            countDownLatch.countDown();
                                            return;
                                        }
                                        if (resp.getTerm() > raftServer.getCurrentTerm().get()) {
                                            countDownLatch.countDown();
                                            return;
                                        } else {
                                            raftServer.setNextIndex(serverId, lastSnapshot.getLastLogIndex() + 1);
                                            raftServer.setMatchIndex(serverId, lastSnapshot.getLastLogIndex());
                                            nextIndex = lastSnapshot.getLastLogIndex() + 1;
                                            entries = raftServer.getLogEntriesSince(lastSnapshot.getLastLogIndex() + 1);
                                            if (entries != null) {
                                                r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(),
                                                        raftServer.getLogEntryTerm(lastSnapshot.getLastLogIndex()), lastSnapshot.getLastLogIndex(), raftServer.getCommitIndex().get(), entries);
                                                response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
                                                if (r.getTerm() != raftServer.getCurrentTerm().get()) {
                                                    countDownLatch.countDown();
                                                    return;
                                                }
                                                if (response.getTerm() > raftServer.getCurrentTerm().get()) {
                                                    logger.warn("Log append failed to " + address + ", not leader anymore");
                                                    raftServer.setState(ServerState.FOLLOWER);
                                                    break;
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if (response.isSuccess() && entries != null) {
                            raftServer.setNextIndex(serverId, nextIndex + entries.length);
                            raftServer.setMatchIndex(serverId, nextIndex + entries.length - 1);
                            successCounter.incrementAndGet();
                        }
                    } else {
                        logger.info("Not leader anymore");
                        raftServer.setState(ServerState.FOLLOWER);
                    }
                }
            } else {
                // Send snapshot
                Snapshot lastSnapshot = raftServer.getLastSnapshot();
                if (lastSnapshot == null) {
                    logger.warn("Entries and snapshots not existing, nothing to bring the follower back");
                } else {
                    InstallSnapshotRequest r = new InstallSnapshotRequest(raftServer.getCurrentTerm().get(), raftServer.getLeaderId(), lastSnapshot);
                    logger.info("Sending snapshot to " + address);
                    InstallSnapshotResponse response = (InstallSnapshotResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
                    if (r.getTerm() != raftServer.getCurrentTerm().get()) {
                        countDownLatch.countDown();
                        return;
                    }
                    if (response == null) {
                        logger.warn("Error when sending snapshot to " + address);
                        new Thread(new AppendLogEntries(raftServer, address, serverId, countDownLatch, successCounter)).start();
                        countDownLatch.countDown();
                        return;
                    }
                    if (response.getTerm() > raftServer.getCurrentTerm().get()) {
                        countDownLatch.countDown();
                        return;
                    } else {
                        successCounter.incrementAndGet();
                    }
                }
            }
            countDownLatch.countDown();
        }
    }
}
