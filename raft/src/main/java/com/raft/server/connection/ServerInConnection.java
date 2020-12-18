package com.raft.server.connection;

import com.raft.server.RaftServer;
import com.raft.server.conf.ServerState;
import com.raft.server.entries.LogEntry;
import com.raft.server.rpc.AppendEntriesRequest;
import com.raft.server.rpc.AppendEntriesResponse;
import com.raft.server.rpc.InstallSnapshotRequest;
import com.raft.server.rpc.InstallSnapshotResponse;
import com.raft.server.rpc.RequestVoteRequest;
import com.raft.server.rpc.RequestVoteResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Incoming connection from other server
 */
public class ServerInConnection implements Runnable {
    private static Logger logger = LogManager.getLogger(ServerInConnection.class);

    private RaftServer server;
    private final Socket connection;
    private final ObjectInputStream in;
    private final ObjectOutputStream out;

    public ServerInConnection(RaftServer server, Socket connection, ObjectInputStream in, ObjectOutputStream out) {
        this.server = server;
        this.connection = connection;
        this.in = in;
        this.out = out;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (!connection.isClosed()) {
                    Object o = in.readObject();
                    handleServerConnection(o, out);
                } else {
                    break;
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                break;
            }
        }
    }

    /**
     * Handle request from other server
     *
     * @param o
     * @param out
     * @throws IOException
     */
    private void handleServerConnection(Object o, ObjectOutputStream out) throws IOException {
        if (RequestVoteRequest.class.equals(o.getClass())) { // Handle vote request
            RequestVoteRequest request = (RequestVoteRequest) o;
            logger.info("Received vote request with term " + request.getTerm() + " from: " + connection.getInetAddress());
            try {
                server.getElectionLock();
                if (request.getTerm() > server.getCurrentTerm() && request.getLastLogIndex() >= server.getLastApplied() && request.getLastLogTerm() >= server.getLastAppliedTerm()) {
                    logger.info("Gave vote to " + request.getSenderId() + " for term " + request.getTerm() + " to: " + connection.getPort());
                    server.getVotedFor().set(request.getSenderId());
                    server.setLastHeardFromLeader(System.nanoTime());
                    server.setState(ServerState.FOLLOWER);
                    server.setCurrentTerm(request.getTerm());
                    logger.info("Sending vote request response to " + connection.getInetAddress());
                    RequestVoteResponse resp = new RequestVoteResponse(server.getCurrentTerm(), true);
                    out.writeObject(resp);
                } else {
                    logger.info("Sending vote request response to " + connection.getInetAddress());
                    RequestVoteResponse resp = new RequestVoteResponse(server.getCurrentTerm(), false);
                    out.writeObject(resp);
                }
            } finally {
                server.releaseElectionLock();
            }
        } else if (AppendEntriesRequest.class.equals(o.getClass())) { // Handle append entries
            logger.debug("Received append ");
            AppendEntriesRequest request = (AppendEntriesRequest) o;
            try {
                server.getAppendLock();
                if (request.getTerm() >= server.getCurrentTerm()) {
                    server.convertToFollower(request);
                    int prevLogIndex = request.getPrevLogIndex();
                    if (prevLogIndex >= 0 && (prevLogIndex >= server.getNextIndex() || server.getLogEntryTerm(prevLogIndex) != request.getPrevLogTerm())) {
                        logger.warn("Inconsistency in log: prevLogIndex " + prevLogIndex + " request prev term: " + request.getPrevLogTerm() + ", entry size: " + request.getEntries().length);
                        out.writeObject(new AppendEntriesResponse(server.getCurrentTerm(), false));
                        return;
                    }
                    LogEntry[] newEntries = request.getEntries();
                    // Add new entries
                    if (newEntries.length > 0) {
                        logger.info("Adding new indices");
                        for (LogEntry entry : newEntries) {
                            // New entries
                            if (entry.getIndex() < server.getNextIndex()) {
                                if (entry.getTerm() != server.getLogEntryTerm(entry.getIndex())) {
                                    server.clearLogFromEntry(entry);
                                    server.addServedRequest(entry.getClientId(),entry.getRequestNr());
                                    server.getLogEntries().add(entry);
                                }
                            } else {
                                server.getLogEntries().add(entry);
                                server.addServedRequest(entry.getClientId(),entry.getRequestNr());
                                server.incrementNextIndex();
                            }
                        }
                    }

                    if (request.getLeaderCommit() > server.getCommitIndex()) {
                        for (int commitInx = server.getCommitIndex() + 1; commitInx <= request.getLeaderCommit() && commitInx < server.getNextIndex(); commitInx++) {
                            logger.info("Commiting inx: " + commitInx);
                            LogEntry entry = server.getLogEntry(commitInx);
                            if (entry != null) {
                                server.applyStateChange(entry.getChange());
                                server.setCommitIndex(commitInx);
                                server.setLastApplied(commitInx);
                                server.setTermOfLastApplied(entry.getTerm());
                            } else {
                                logger.error("Entry not in log");
                            }
                        }
                    }

                    out.writeObject(new AppendEntriesResponse(server.getCurrentTerm(), true));
                } else {
                    logger.info("Append from old term");
                    out.writeObject(new AppendEntriesResponse(server.getCurrentTerm(), false));
                }
            } finally {
                server.releaseAppendLock();
            }
        } else if (InstallSnapshotRequest.class.equals(o.getClass())) {
            InstallSnapshotRequest request = (InstallSnapshotRequest) o;
            logger.info("Received snapshot");
            try {
                server.getAppendLock();
                if (request.getTerm() >= server.getCurrentTerm()) {
                    server.setCurrentTerm(request.getTerm());
                    server.setState(ServerState.FOLLOWER);
                    server.setLeaderId(request.getSenderId());
                    server.setLastHeardFromLeader(System.nanoTime());
                    if (request.getSnapshot().getLastLogIndex() >= server.getNextIndex()) {
                        server.setMachineState(request.getSnapshot().getStorage());
                        server.setServersState(request.getSnapshot().getServersConfiguration());
                        server.setNextIndex(request.getSnapshot().getLastLogIndex() + 1);
                        server.setTermOfLastApplied(request.getSnapshot().getLastTerm());
                        server.setLastApplied(request.getSnapshot().getLastLogIndex());
                        server.setCommitIndex(request.getSnapshot().getLastLogIndex());
                        server.setLastSnapshot(request.getSnapshot());
                        if (server.getCurrentTerm() < request.getSnapshot().getLastTerm()) {
                            server.setCurrentTerm(request.getSnapshot().getLastTerm());
                        }
                        logger.info("Set Last Applied to " + request.getSnapshot().getLastLogIndex());
                    } else {
                        logger.info("Request is to index: " + request.getSnapshot().getLastLogIndex() + " my next index is" + server.getNextIndex());
                        // Prefix of log
                        server.removeEntriesUntil(request.getSnapshot().getLastLogIndex());
                    }
                    out.writeObject(new InstallSnapshotResponse(server.getCurrentTerm()));
                } else {
                    logger.warn("Got outdated snapshot with term: " + request.getTerm() + " current term: " + server.getCurrentTerm());
                    out.writeObject(new InstallSnapshotResponse(server.getCurrentTerm()));
                }
            } finally {
                server.releaseAppendLock();
            }
        }
    }
}
