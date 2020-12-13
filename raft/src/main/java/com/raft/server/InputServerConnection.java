package com.raft.server;

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

public class InputServerConnection implements Runnable {
    private static Logger logger = LogManager.getLogger(InputServerConnection.class);

    private RaftServer server;
    private final Socket connection;
    private final ObjectInputStream in;
    private final ObjectOutputStream out;

    public InputServerConnection(RaftServer server, Socket connection, ObjectInputStream in, ObjectOutputStream out) {
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

    private void handleServerConnection(Object o, ObjectOutputStream out) throws IOException {
        if (o.getClass().equals(RequestVoteRequest.class)) {
            RequestVoteRequest request = (RequestVoteRequest) o;
            logger.info("Received vote request with term " + request.getTerm() + " from: " + connection.getInetAddress());
            if (request.getTerm() > server.getCurrentTerm().get() && request.getLastLogIndex() >= server.getLastApplied().get() && (server.getLogEntries().size() == 0 || request.getLastLogTerm() == server.getLogEntries().get(server.getLastApplied().get()).getTerm())) {
                try{
                    server.getElectionLock();
                    logger.info("Gave vote to " + request.getCandidateId() + " for term " + request.getTerm() + " to: " + connection.getPort());
                    server.getVotedFor().set(request.getCandidateId());
                    server.setLastHeardFromLeader(System.nanoTime());
                    server.setState(ServerState.FOLLOWER);
                    server.getCurrentTerm().set(request.getTerm());
                    logger.info("Sending vote request response to " + connection.getInetAddress());
                    RequestVoteResponse resp = new RequestVoteResponse(server.getCurrentTerm().get(), true);
                    out.writeObject(resp);
                } finally {
                    server.releaseElectionLock();
                }
            } else {
                logger.info("Sending vote request response to " + connection.getInetAddress());
                RequestVoteResponse resp = new RequestVoteResponse(server.getCurrentTerm().get(), false);
                out.writeObject(resp);
            }
        } else if (AppendEntriesRequest.class.equals(o.getClass())) {
            logger.debug("Received append ");
            AppendEntriesRequest request = (AppendEntriesRequest) o;
            if (request.getTerm() >= server.getCurrentTerm().get()) {
                server.getCurrentTerm().set(request.getTerm());
                server.setState(ServerState.FOLLOWER);
                server.setLeaderId(request.getLeaderId());
                int prevLogIndex = request.getPrevLogIndex();
                server.setLastHeardFromLeader(System.nanoTime());
                if (prevLogIndex >= 0 && (prevLogIndex >= server.getLogEntries().size() || server.getLogEntries().get(prevLogIndex).getTerm() != request.getPrevLogTerm())) {
                    logger.warn("Inconsistency in log: prevLogIndex " + prevLogIndex + " request prev term: " + request.getPrevLogTerm());
                    out.writeObject(new AppendEntriesResponse(server.getCurrentTerm().get(), false));
                    return;
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
                    for (int commitInx = server.getCommitIndex().get(); commitInx < request.getLeaderCommit() && commitInx < server.getLogEntries().size(); commitInx++) {
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
        }
    }
}
