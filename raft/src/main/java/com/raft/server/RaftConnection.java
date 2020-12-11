package com.raft.server;

import com.raft.requests.ChangeStateRequest;
import com.raft.requests.ChangeStateResponse;
import com.raft.server.jobs.RequestVote;
import com.raft.server.rpc.AppendEntriesRequest;
import com.raft.server.rpc.AppendEntriesResponse;
import com.raft.server.rpc.RequestVoteRequest;
import com.raft.server.rpc.RequestVoteResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.print.attribute.standard.PresentationDirection;
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

            if (o.getClass().equals(RequestVoteRequest.class)) {
                logger.info("Received request vote ");
                RequestVoteRequest request = (RequestVoteRequest) o;
                if (request.getTerm() > server.getCurrentTerm().get() && request.getLastLogIndex() >= server.getLastApplied().get()) {
                    logger.info("Gave vote to " + request.getCandidateId());
                    server.getVotedFor().set(request.getCandidateId());
                    server.setLastHeardFromLeader(System.nanoTime());
                    server.setState(ServerState.FOLLOWER);
                    server.getCurrentTerm().set(request.getTerm());
                    RequestVoteResponse resp = new RequestVoteResponse(server.getCurrentTerm().get(), true);
                    out.writeObject(resp);
                } else {
                    RequestVoteResponse resp = new RequestVoteResponse(server.getCurrentTerm().get(), false);
                    out.writeObject(resp);
                }
            } else if (AppendEntriesRequest.class.equals(o.getClass())) {
                logger.info("Received append ");
                AppendEntriesRequest request = (AppendEntriesRequest) o;
                if (request.getTerm() >= server.getCurrentTerm().get()) {
                    server.getCurrentTerm().set(request.getTerm());
                    server.setState(ServerState.FOLLOWER);
                    server.setLeaderId(request.getLeaderId());
                    server.setLastHeardFromLeader(System.nanoTime());
                    out.writeObject(new AppendEntriesResponse(server.getCurrentTerm().get(),true));
                } else {
                    out.writeObject(new AppendEntriesResponse(server.getCurrentTerm().get(),false));
                }
            } else if (ChangeStateRequest.class.equals(o.getClass())) {
                if (server.getState() == ServerState.LEADER) {
                    ChangeStateRequest r = (ChangeStateRequest) o;
                    server.modifyLog(r.getVar(), r.getValue());
                    ChangeStateResponse changeStateResponse = new ChangeStateResponse(server.getServerId(), true);
                    out.writeObject(changeStateResponse);
                } else {
                    ChangeStateResponse changeStateResponse = new ChangeStateResponse(server.getLeaderId(), false);
                    out.writeObject(changeStateResponse);
                }
            } else {
                logger.info("Received: " + o.getClass().getName());
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (out != null) {
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
