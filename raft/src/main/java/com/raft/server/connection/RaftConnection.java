package com.raft.server.connection;

import com.raft.requests.*;
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

public class RaftConnection implements Runnable {
    private static Logger logger = LogManager.getLogger(RaftConnection.class);

    Socket connection;
    final RaftServer server;

    public RaftConnection(Socket accept, RaftServer raftServer) {
        connection = accept;
        server = raftServer;
    }

    @Override
    public void run() {
        ObjectInputStream in = null;
        ObjectOutputStream out = null;
        try {
            in = new ObjectInputStream(connection.getInputStream());
            out = new ObjectOutputStream(connection.getOutputStream());
            Object o = in.readObject();
            if (handleServerConnection(o, out)) {
                new Thread(new InputServerConnection(server, connection, in, out)).start();
                return;
            }
            if (ChangeStateRequest.class.equals(o.getClass())) {
                logger.info("Serving State change from client");
                if (server.getState() == ServerState.LEADER) {
                    logger.info("Responding to State change from client");
                    ChangeStateRequest r = (ChangeStateRequest) o;
                    AckResponse changeStateResponse = server.modifyLog(r);
                    out.writeObject(changeStateResponse);
                } else {
                    logger.info("Not leader, send leader address to client");
                    AckResponse changeStateResponse = new AckResponse(server.getLeaderAddress(), false, ErrorCause.NOT_LEADER);
                    out.writeObject(changeStateResponse);
                }
            } else if (ReadRequest.class.equals(o.getClass())) {
                logger.info("Serving read request from client");
                if (server.getState() == ServerState.LEADER) {
                    logger.info("Responding to read from client");
                    ReadRequest r = (ReadRequest) o;
                    ReadResponse readResponse = server.readFromState(r);
                    out.writeObject(readResponse);
                } else {
                    logger.info("Not leader, send leader address to client");
                    ReadResponse readResponse = new ReadResponse(server.getLeaderAddress(), false, -1);
                    out.writeObject(readResponse);
                }
            } else if (AddServerRequest.class.equals(o.getClass())) {
                logger.info("Serving add new server from client");
                if (server.getState() == ServerState.LEADER) {
                    logger.info("Responding add new server from client");
                    AddServerRequest r = (AddServerRequest) o;
                    AckResponse changeStateResponse = server.addNewServer(r);
                    out.writeObject(changeStateResponse);
                } else {
                    logger.info("Not leader, send leader address to client");
                    AckResponse changeStateResponse = new AckResponse(server.getLeaderAddress(), false, ErrorCause.NOT_LEADER);
                    out.writeObject(changeStateResponse);
                }
            } else if (RemoveServerRequest.class.equals(o.getClass())) {
                logger.info("Serving remove server request from client");
                if (server.getState() == ServerState.LEADER) {
                    logger.info("Responding remove server from client");
                    RemoveServerRequest r = (RemoveServerRequest) o;
                    AckResponse readResponse = server.removeServer(r);
                    out.writeObject(readResponse);
                } else {
                    logger.info("Not leader, send leader address to client");
                    AckResponse readResponse = new AckResponse(server.getLeaderAddress(), false, ErrorCause.NOT_LEADER);
                    out.writeObject(readResponse);
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

    private boolean handleServerConnection(Object o, ObjectOutputStream out) throws IOException {
        if (o.getClass().equals(RequestVoteRequest.class)) {
            RequestVoteRequest request = (RequestVoteRequest) o;
            logger.info("Received vote request with term " + request.getTerm() + " from: " + connection.getInetAddress());
            try {
                server.getElectionLock();
                if (request.getTerm() > server.getCurrentTerm().get() && request.getLastLogIndex() >= server.getLastApplied().get() && request.getLastLogTerm() >= server.getLastAppliedTerm()) {
                    logger.info("Gave vote to " + request.getCandidateId() + " for term " + request.getTerm() + " to: " + connection.getPort());
                    server.getVotedFor().set(request.getCandidateId());
                    server.setLastHeardFromLeader(System.nanoTime());
                    server.setState(ServerState.FOLLOWER);
                    server.getCurrentTerm().set(request.getTerm());
                    logger.info("Sending vote request response to " + connection.getInetAddress());
                    RequestVoteResponse resp = new RequestVoteResponse(server.getCurrentTerm().get(), true);
                    out.writeObject(resp);

                } else {
                    logger.info("Sending vote request response to " + connection.getInetAddress());
                    RequestVoteResponse resp = new RequestVoteResponse(server.getCurrentTerm().get(), false);
                    out.writeObject(resp);
                }
            } finally {
                server.releaseElectionLock();
            }
            return true;
        } else if (AppendEntriesRequest.class.equals(o.getClass())) {
            logger.debug("Received append ");
            AppendEntriesRequest request = (AppendEntriesRequest) o;
            try {
                server.getAppendLock();
                if (request.getTerm() >= server.getCurrentTerm().get()) {
                    server.getCurrentTerm().set(request.getTerm());
                    server.setState(ServerState.FOLLOWER);
                    server.setLeaderId(request.getLeaderId());
                    int prevLogIndex = request.getPrevLogIndex();
                    server.setLastHeardFromLeader(System.nanoTime());
                    if (prevLogIndex >= 0 && (prevLogIndex >= server.getNextIndex() || server.getTermForEntry(prevLogIndex) != request.getPrevLogTerm())) {
                        logger.warn("Inconsistency in log: prevLogIndex " + prevLogIndex + " request prev term: " + request.getPrevLogTerm());
                        out.writeObject(new AppendEntriesResponse(server.getCurrentTerm().get(), false));
                        return true;
                    }
                    LogEntry[] newEntries = request.getEntries();
                    if (newEntries.length > 0) {
                        logger.info("Adding new indices");
                        for (LogEntry entry : newEntries) {
                            // New entries
                            if (entry.getIndex() < server.getNextIndex()) {
                                if (entry.getTerm() != server.getLogEntryTerm(entry.getIndex())) {
                                    server.clearLogFromEntry(entry);
                                    server.getLogEntries().add(entry);
                                }
                            } else {
                                server.getLogEntries().add(entry);
                                server.incrementNextIndex();
                            }
                        }
                    }

                    if (request.getLeaderCommit() > server.getCommitIndex().get()) {
                        for (int commitInx = server.getCommitIndex().get() + 1; commitInx <= request.getLeaderCommit() && commitInx < server.getNextIndex(); commitInx++) {
                            logger.info("Commiting inx: " + commitInx);
                            LogEntry entry = server.getLogEntry(commitInx);
                            if (entry != null) {
                                server.applyStateChange(entry.getChange());
                                server.getCommitIndex().set(commitInx);
                                server.getLastApplied().set(commitInx);
                                server.setTermOfLastApplied(entry.getTerm());
                            } else {
                                logger.error("Entry not in log");
                            }
                        }
                    }

                    out.writeObject(new AppendEntriesResponse(server.getCurrentTerm().get(), true));
                } else {
                    out.writeObject(new AppendEntriesResponse(server.getCurrentTerm().get(), false));
                }
            } finally {
                server.releaseAppendLock();
            }
            return true;
        } else if (InstallSnapshotRequest.class.equals(o.getClass())) {
            InstallSnapshotRequest request = (InstallSnapshotRequest) o;
            logger.info("Received snapshot");
            try {
                server.getAppendLock();
                if (request.getTerm() >= server.getCurrentTerm().get()) {
                    server.getCurrentTerm().set(request.getTerm());
                    server.setState(ServerState.FOLLOWER);
                    server.setLeaderId(request.getLeaderId());
                    server.setLastHeardFromLeader(System.nanoTime());
                    if (request.getSnapshot().getLastLogIndex() > server.getNextIndex()) {
                        server.setMachineState(request.getSnapshot().getState());
                        server.setServersState(request.getSnapshot().getServersConfiguration());
                        server.setNextIndex(request.getSnapshot().getLastLogIndex() + 1);
                        server.setTermOfLastApplied(request.getSnapshot().getLastTerm());
                        server.getLastApplied().set(request.getSnapshot().getLastLogIndex());
                        server.getCommitIndex().set(request.getSnapshot().getLastLogIndex());
                        server.setLastSnapshot(request.getSnapshot());
                        if (server.getCurrentTerm().get() < request.getSnapshot().getLastTerm()) {
                            server.getCurrentTerm().set(request.getSnapshot().getLastTerm());
                        }
                        logger.info("Set Last Applied to " + request.getSnapshot().getLastLogIndex());
                    } else {
                        // Prefix of log
                        server.removeEntriesUntil(request.getSnapshot().getLastLogIndex());
                    }
                    out.writeObject(new InstallSnapshotResponse(server.getCurrentTerm().get()));
                } else {
                    logger.warn("Got outdated snapshot with term: " + request.getTerm() + " current term: " + server.getCurrentTerm().get());
                    out.writeObject(new InstallSnapshotResponse(server.getCurrentTerm().get()));
                }
            } finally {
                server.releaseAppendLock();
            }
            return true;
        }
        return false;
    }
}
