package com.raft.server;

import com.raft.requests.ChangeStateRequest;
import com.raft.requests.ChangeStateResponse;
import com.raft.requests.ReadRequest;
import com.raft.requests.ReadResponse;
import com.raft.server.rpc.AppendEntriesRequest;
import com.raft.server.rpc.AppendEntriesResponse;
import com.raft.server.rpc.RequestVoteRequest;
import com.raft.server.rpc.RequestVoteResponse;
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
                    ChangeStateResponse changeStateResponse = server.modifyLog(r);
                    out.writeObject(changeStateResponse);
                } else {
                    logger.info("Not leader, send leader address to client");
                    ChangeStateResponse changeStateResponse = new ChangeStateResponse(server.getLeaderAddress(), false);
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
            } else {
                logger.warn("Received: " + o.getClass().getName());
            }
            in.close();
            out.close();
            connection.close();
        } catch (IOException | ClassNotFoundException e) {
            logger.error("Error when executing socket: ", e);
            e.printStackTrace();
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
                ex.printStackTrace();
            }
        }
    }

    private boolean handleServerConnection(Object o, ObjectOutputStream out) throws IOException {
        if (o.getClass().equals(RequestVoteRequest.class)) {
            RequestVoteRequest request = (RequestVoteRequest) o;
            logger.info("Received vote request with term " + request.getTerm() + " from: " + connection.getInetAddress());
            try {
                server.getElectionLock();
                if (request.getTerm() > server.getCurrentTerm().get() && request.getLastLogIndex() >= server.getLastApplied().get() && (server.getLogEntries().size() == 0 || request.getLastLogTerm() == server.getLogEntries().get(server.getLastApplied().get()).getTerm())) {
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
                    if (prevLogIndex >= 0 && (prevLogIndex >= server.getLogEntries().size() || server.getLogEntries().get(prevLogIndex).getTerm() != request.getPrevLogTerm())) {
                        logger.warn("Inconsistency in log: prevLogIndex " + prevLogIndex + " request prev term: " + request.getPrevLogTerm());
                        out.writeObject(new AppendEntriesResponse(server.getCurrentTerm().get(), false));
                        return true;
                    }
                    LogEntry[] newEntries = request.getEntries();
                    if (newEntries.length > 0) {
                        logger.info("Adding new indices");
                        int workedLogEntryIndex = request.getPrevLogIndex() + 1;
                        for (LogEntry entry : newEntries) {
                            if (server.getLogEntries().size() > workedLogEntryIndex) {
                                LogEntry existingEntry = server.getLogEntries().get(workedLogEntryIndex);
                                if (existingEntry.getTerm() != entry.getTerm()) {
                                    server.getLogEntries().set(workedLogEntryIndex, entry);
                                }
                            } else {
                                server.getLogEntries().add(entry);
                            }
                        }
                    }

                    if (request.getLeaderCommit() > server.getCommitIndex().get()) {
                        for (int commitInx = server.getCommitIndex().get() + 1; commitInx <= request.getLeaderCommit() && commitInx < server.getLogEntries().size(); commitInx++) {
                            logger.info("Commiting inx: " + commitInx);
                            LogEntry entry = server.getLogEntries().get(commitInx);
                            server.applyStateChange(entry.getChange());
                            server.getCommitIndex().set(commitInx);
                            server.getLastApplied().set(commitInx);
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
        }
        return false;
    }
}
