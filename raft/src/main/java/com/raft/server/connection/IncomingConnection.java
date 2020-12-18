package com.raft.server.connection;

import com.raft.requests.AckResponse;
import com.raft.requests.AddServerRequest;
import com.raft.requests.ChangeStateRequest;
import com.raft.requests.ErrorCause;
import com.raft.requests.ReadRequest;
import com.raft.requests.ReadResponse;
import com.raft.requests.RemoveServerRequest;
import com.raft.server.RaftServer;
import com.raft.server.conf.ServerState;
import com.raft.server.entries.LogEntry;
import com.raft.server.rpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Handles a new connections to the server
 */
public class IncomingConnection implements Runnable {
    private static Logger logger = LogManager.getLogger(IncomingConnection.class);

    /**
     * Incoming connection
     */
    private Socket connection;

    /**
     * Raft server
     */
    private final RaftServer raftServer;

    public IncomingConnection(Socket accept, RaftServer raftServer) {
        connection = accept;
        this.raftServer = raftServer;
    }

    @Override
    public void run() {
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        try {
            in = new ObjectInputStream(connection.getInputStream());
            out = new ObjectOutputStream(connection.getOutputStream());
            Object o = in.readObject();
            // Check if connection from other server
            if (handleServerConnection(o, out)) {
                new Thread(new ServerInConnection(raftServer, connection, in, out)).start();
                return;
            }
            // Store value in storage
            if (ChangeStateRequest.class.equals(o.getClass())) {
                logger.info("Serving State change from client");
                if (raftServer.getState() == ServerState.LEADER) {
                    logger.info("Responding to State change from client");
                    ChangeStateRequest r = (ChangeStateRequest) o;
                    AckResponse response = raftServer.modifyLog(r);
                    out.writeObject(response);
                } else {
                    logger.info("Not leader, send leader address to client");
                    AckResponse response = new AckResponse(raftServer.getLeaderAddress(), false, ErrorCause.NOT_LEADER);
                    out.writeObject(response);
                }
            } else if (ReadRequest.class.equals(o.getClass())) { // Read variable from storage
                logger.info("Serving read request from client");
                if (raftServer.getState() == ServerState.LEADER) {
                    logger.info("Responding to read from client");
                    ReadRequest r = (ReadRequest) o;
                    ReadResponse readResponse = raftServer.readFromState(r);
                    out.writeObject(readResponse);
                } else {
                    logger.info("Not leader, send leader address to client");
                    ReadResponse readResponse = new ReadResponse(raftServer.getLeaderAddress(), false, ErrorCause.NOT_LEADER);
                    out.writeObject(readResponse);
                }
            } else if (AddServerRequest.class.equals(o.getClass())) { // Add new server to configuration
                logger.info("Serving add new server from client");
                if (raftServer.getState() == ServerState.LEADER) {
                    logger.info("Responding add new server from client");
                    AddServerRequest r = (AddServerRequest) o;
                    AckResponse response = raftServer.addNewServer(r);
                    out.writeObject(response);
                } else {
                    logger.info("Not leader, send leader address to client");
                    AckResponse response = new AckResponse(raftServer.getLeaderAddress(), false, ErrorCause.NOT_LEADER);
                    out.writeObject(response);
                }
            } else if (RemoveServerRequest.class.equals(o.getClass())) { // Remove server from cluster
                logger.info("Serving remove server request from client");
                if (raftServer.getState() == ServerState.LEADER) {
                    logger.info("Responding remove server from client");
                    RemoveServerRequest r = (RemoveServerRequest) o;
                    AckResponse response = raftServer.removeServer(r);
                    out.writeObject(response);
                } else {
                    logger.info("Not leader, send leader address to client");
                    AckResponse response = new AckResponse(raftServer.getLeaderAddress(), false, ErrorCause.NOT_LEADER);
                    out.writeObject(response);
                }
            } else {
                logger.warn("Received: " + o.getClass().getName());
            }
            in.close();
            out.close();
            connection.close();
        } catch (IOException | ClassNotFoundException e) {
            logger.info("Error when executing socket: ", e);
            try {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
                connection.close();
            } catch (IOException ex) {
                logger.error("Error when closing socket: ", ex);
            }
        }
    }

    /**
     * Check if received object is sent by server and handle it
     *
     * @param requestObject received object
     * @param out           out stream
     * @return True, if received request from other server.
     * @throws IOException
     */
    private boolean handleServerConnection(Object requestObject, ObjectOutputStream out) throws IOException {
        // Check if vote request received
        if (requestObject.getClass().equals(RequestVoteRequest.class)) {
            RequestVoteRequest request = (RequestVoteRequest) requestObject;
            logger.info("Received vote request with term " + request.getTerm() + " from: " + connection.getInetAddress());
            try {
                // Take lock
                raftServer.getElectionLock();
                // Check if candidate has bigger term and that log is as up to date as current server
                if (request.getTerm() > raftServer.getCurrentTerm() && request.getLastLogIndex() >= raftServer.getLastApplied() && request.getLastLogTerm() >= raftServer.getLastAppliedTerm()) {
                    logger.info("Gave vote to " + request.getSenderId() + " for term " + request.getTerm() + " to: " + connection.getPort());
                    raftServer.getVotedFor().set(request.getSenderId());
                    raftServer.setLastHeardFromLeader(System.nanoTime());
                    raftServer.setState(ServerState.FOLLOWER);
                    raftServer.setCurrentTerm(request.getTerm());
                    logger.info("Sending vote request response to " + connection.getInetAddress());
                    RequestVoteResponse resp = new RequestVoteResponse(raftServer.getCurrentTerm(), true);
                    out.writeObject(resp);
                } else {
                    logger.info("Sending vote request response to " + connection.getInetAddress());
                    RequestVoteResponse resp = new RequestVoteResponse(raftServer.getCurrentTerm(), false);
                    out.writeObject(resp);
                }
            } finally {
                raftServer.releaseElectionLock();
            }
            return true;
        } else if (AppendEntriesRequest.class.equals(requestObject.getClass())) { // Append log entries request
            logger.debug("Received append ");
            AppendEntriesRequest request = (AppendEntriesRequest) requestObject;
            try {
                // Get append lock
                raftServer.getAppendLock();
                if (request.getTerm() >= raftServer.getCurrentTerm()) {
                    raftServer.convertToFollower(request);
                    int prevLogIndex = request.getPrevLogIndex();
                    // Check if logs match with leader
                    if (prevLogIndex >= 0 && (prevLogIndex >= raftServer.getNextIndex() || raftServer.getLogEntryTerm(prevLogIndex) != request.getPrevLogTerm())) {
                        logger.warn("Inconsistency in log: prevLogIndex " + prevLogIndex + " request prev term: " + request.getPrevLogTerm());
                        out.writeObject(new AppendEntriesResponse(raftServer.getCurrentTerm(), false));
                        return true;
                    }
                    LogEntry[] newEntries = request.getEntries();
                    // Add new log entries to log
                    if (newEntries.length > 0) {
                        logger.info("Adding new indices");
                        for (LogEntry entry : newEntries) {
                            // New entries
                            if (entry.getIndex() < raftServer.getNextIndex()) {
                                if (entry.getTerm() != raftServer.getLogEntryTerm(entry.getIndex())) {
                                    raftServer.clearLogFromEntry(entry);
                                    raftServer.getLogEntries().add(entry);
                                    raftServer.addServedRequest(entry.getClientId(),entry.getRequestNr());
                                }
                            } else {
                                raftServer.getLogEntries().add(entry);
                                raftServer.incrementNextIndex();
                                raftServer.addServedRequest(entry.getClientId(),entry.getRequestNr());
                            }
                        }
                    }
                    // Commit the log entries
                    if (request.getLeaderCommit() > raftServer.getCommitIndex()) {
                        for (int indexToCommit = raftServer.getCommitIndex() + 1;
                             indexToCommit <= request.getLeaderCommit() && indexToCommit < raftServer.getNextIndex(); indexToCommit++) {
                            logger.info("Commiting inx: " + indexToCommit);
                            raftServer.commitEntry(indexToCommit);
                        }
                    }

                    out.writeObject(new AppendEntriesResponse(raftServer.getCurrentTerm(), true));
                } else {
                    out.writeObject(new AppendEntriesResponse(raftServer.getCurrentTerm(), false));
                }
            } finally {
                raftServer.releaseAppendLock();
            }
            return true;
        } else if (InstallSnapshotRequest.class.equals(requestObject.getClass())) { // Install the snapshot sent by leader
            InstallSnapshotRequest request = (InstallSnapshotRequest) requestObject;
            logger.info("Received snapshot");
            try {
                raftServer.getAppendLock();
                if (request.getTerm() >= raftServer.getCurrentTerm()) {
                    raftServer.convertToFollower(request);
                    if (request.getSnapshot().getLastLogIndex() > raftServer.getNextIndex()) {
                        raftServer.setMachineState(request.getSnapshot().getStorage());
                        raftServer.setServersState(request.getSnapshot().getServersConfiguration());
                        raftServer.setNextIndex(request.getSnapshot().getLastLogIndex() + 1);
                        raftServer.setTermOfLastApplied(request.getSnapshot().getLastTerm());
                        raftServer.setLastApplied(request.getSnapshot().getLastLogIndex());
                        raftServer.setCommitIndex(request.getSnapshot().getLastLogIndex());
                        raftServer.setLastSnapshot(request.getSnapshot());
                        raftServer.setServedClients(request.getSnapshot().getServedClients());
                        if (raftServer.getCurrentTerm() < request.getSnapshot().getLastTerm()) {
                            raftServer.setCurrentTerm(request.getSnapshot().getLastTerm());
                        }
                        logger.info("Set Last Applied to " + request.getSnapshot().getLastLogIndex());
                    } else {
                        // Prefix of log
                        raftServer.removeEntriesUntil(request.getSnapshot().getLastLogIndex());
                    }
                    out.writeObject(new InstallSnapshotResponse(raftServer.getCurrentTerm()));
                } else {
                    logger.warn("Got outdated snapshot with term: " + request.getTerm() + " current term: " + raftServer.getCurrentTerm());
                    out.writeObject(new InstallSnapshotResponse(raftServer.getCurrentTerm()));
                }
            } finally {
                raftServer.releaseAppendLock();
            }
            return true;
        }
        return false;
    }
}
