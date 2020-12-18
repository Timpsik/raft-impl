package com.raft.server.jobs;

import com.raft.server.conf.Configuration;
import com.raft.server.entries.LogEntry;
import com.raft.server.RaftServer;
import com.raft.server.conf.ServerState;
import com.raft.server.rpc.*;
import com.raft.server.snapshot.Snapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Heartbeat to other server
 */
public class Heartbeat implements Runnable {
    private static Logger logger = LogManager.getLogger(Heartbeat.class);

    private final RaftServer raftServer;

    /**
     * Address of the other server
     */
    private final String address;

    /**
     * Id of the other server
     */
    private final int serverId;

    public Heartbeat(RaftServer raftServer, String address, int serverId) {
        this.raftServer = raftServer;
        this.address = address;
        this.serverId = serverId;
    }

    @Override
    public void run() {
        // Send if in leader state
        if (raftServer.getState() == ServerState.LEADER) {
            AppendEntriesRequest r = new AppendEntriesRequest(raftServer.getCurrentTerm(), raftServer.getServerId(),
                    raftServer.getLastLogEntryTerm(), raftServer.getNextIndex() - 1, raftServer.getCommitIndex(), new LogEntry[0]);
            // Send the request to other server
            AppendEntriesResponse response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
            if (response == null) {
                raftServer.getScheduler().schedule(new Heartbeat(raftServer, address, serverId), Configuration.minElectionTimeout / 2, TimeUnit.MILLISECONDS);
                return;
            }
            // Term change do not process the result
            if (response.getTerm() != raftServer.getCurrentTerm()) {
                return;
            }

            // Heartbeat failed, try to fix the follower log
            if (!response.isSuccess()) {
                logger.warn("Heartbeat failed to " + address);
                if (response.getTerm() > raftServer.getCurrentTerm()) {
                    raftServer.setState(ServerState.FOLLOWER);
                    return;
                } else {
                    while (!response.isSuccess()) {
                        if (raftServer.getState() == ServerState.LEADER) {
                            // Reduce the next index
                            int nextIndex = raftServer.reduceAndGetNextIndex(serverId);
                            // Get log entries to send to client
                            LogEntry[] entries = raftServer.getLogEntriesSince(nextIndex);
                            if (entries != null) {
                                logger.warn("Fixing followers log, next Index is:  " + nextIndex + ", sending " + entries.length + " entries");
                                r = new AppendEntriesRequest(raftServer.getCurrentTerm(), raftServer.getServerId(),
                                        raftServer.getLogEntryTerm(nextIndex - 1), nextIndex - 1,
                                        raftServer.getCommitIndex(), entries);

                                response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
                                if (response == null) {
                                    raftServer.getScheduler().schedule(new Heartbeat(raftServer, address, serverId),
                                            Configuration.minElectionTimeout / 2, TimeUnit.MILLISECONDS);
                                    return;
                                }
                                if (response.getTerm() != raftServer.getCurrentTerm()) {
                                    return;
                                }
                                if (response.getTerm() > raftServer.getCurrentTerm()) {
                                    logger.warn("Log append failed to " + address + ", not leader anymore");
                                    raftServer.setState(ServerState.FOLLOWER);
                                    return;
                                }
                                // Update next and match index
                                if (response.isSuccess()) {
                                    raftServer.setNextIndex(serverId, nextIndex + entries.length);
                                    raftServer.setMatchIndex(serverId, nextIndex + entries.length - 1);
                                }
                            } else {
                                // Use snapshot to bring the follower back
                                Snapshot lastSnapshot = raftServer.getLastSnapshot();
                                if (lastSnapshot == null) {
                                    logger.warn("Entries and snapshots not existing, nothing to bring the follower back");
                                } else {
                                    InstallSnapshotRequest req = new InstallSnapshotRequest(raftServer.getCurrentTerm(), raftServer.getLeaderId(), lastSnapshot);
                                    logger.info("Sending snapshot to " + address);
                                    InstallSnapshotResponse resp = (InstallSnapshotResponse) raftServer.getServerConnection(serverId).sendRequestToServer(req);
                                    if (r.getTerm() != raftServer.getCurrentTerm()) {
                                        return;
                                    }
                                    if (resp == null) {
                                        logger.warn("Error when sending snapshot to " + address);
                                        raftServer.getScheduler().schedule(new Heartbeat(raftServer, address, serverId), Configuration.minElectionTimeout / 2, TimeUnit.MILLISECONDS);
                                        return;
                                    }
                                    if (resp.getTerm() <= raftServer.getCurrentTerm()) {
                                        raftServer.setNextIndex(serverId, lastSnapshot.getLastLogIndex() + 1);
                                        raftServer.setMatchIndex(serverId, lastSnapshot.getLastLogIndex());
                                        nextIndex = lastSnapshot.getLastLogIndex() + 1;
                                        entries = raftServer.getLogEntriesSince(lastSnapshot.getLastLogIndex() + 1);
                                        // Check if there are more log entries to send to follower to bring it to up to date
                                        if (entries != null) {
                                            r = new AppendEntriesRequest(raftServer.getCurrentTerm(), raftServer.getServerId(),
                                                    raftServer.getLogEntryTerm(lastSnapshot.getLastLogIndex()), lastSnapshot.getLastLogIndex(),
                                                    raftServer.getCommitIndex(), entries);
                                            response = (AppendEntriesResponse) raftServer.getServerConnection(serverId).sendRequestToServer(r);
                                            if (r.getTerm() != raftServer.getCurrentTerm()) {
                                                return;
                                            }
                                            if (response.getTerm() > raftServer.getCurrentTerm()) {
                                                logger.warn("Log append failed to " + address + ", not leader anymore");
                                                raftServer.setState(ServerState.FOLLOWER);
                                                break;
                                            }
                                            // Update next index for follower
                                            if (response.isSuccess()) {
                                                raftServer.setNextIndex(serverId, nextIndex + entries.length);
                                                raftServer.setMatchIndex(serverId, nextIndex + entries.length - 1);
                                            }
                                        } else {
                                            // Update next index for follower
                                            raftServer.setNextIndex(serverId, lastSnapshot.getLastLogIndex() + 1);
                                            raftServer.setMatchIndex(serverId, lastSnapshot.getLastLogIndex());
                                        }
                                        break;
                                    }
                                }
                            }
                        }
                    }

                }
            }
            raftServer.getScheduler().schedule(new Heartbeat(raftServer, address, serverId), Configuration.minElectionTimeout / 2, TimeUnit.MILLISECONDS);
        }
    }
}
