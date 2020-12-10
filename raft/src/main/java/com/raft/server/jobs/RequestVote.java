package com.raft.server.jobs;

import com.raft.server.RaftServer;
import com.raft.server.rpc.RequestVoteRequest;
import com.raft.server.rpc.RequestVoteResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.Socket;

public class RequestVote implements Runnable {
    private static Logger logger = LogManager.getLogger(RequestVote.class);


    private final RaftServer raftServer;
    private final String address;

    public RequestVote(RaftServer raftServer, String address) {
        this.raftServer = raftServer;
        this.address = address;
    }

    @Override
    public void run() {
        RequestVoteRequest r = new RequestVoteRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(), raftServer.getCommitIndex().get(), raftServer.getLastLogEntryTerm());
        logger.info("Sending vote request to " + address);
        Socket socket = null;
        try {
            socket = new Socket(address.split(":")[0], Integer.parseInt(address.split(":")[1]));
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(r);
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            RequestVoteResponse response = (RequestVoteResponse) in.readObject();
            if (response.isVoteGranted()) {
                logger.info("Received vote from " + address);
                raftServer.addVoteAndCheckWin();
            } else {
                logger.info("Did not get vote from " + address);
            }

        } catch (IOException | ClassNotFoundException e) {
            logger.error("Error when sending vote request to " + address + ": ", e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
