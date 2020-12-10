package com.raft.server.jobs;

import com.raft.server.Configuration;
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
import java.util.concurrent.TimeUnit;

public class Heartbeat implements Runnable {
    private static Logger logger = LogManager.getLogger(Heartbeat.class);

    private final RaftServer raftServer;
    private final String address;

    public Heartbeat(RaftServer raftServer, String address) {
        this.raftServer = raftServer;
        this.address = address;
    }

    @Override
    public void run() {
        if (raftServer.getState() == ServerState.LEADER) {
            AppendEntriesRequest r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(), raftServer.getLastLogEntryTerm(), raftServer.getCommitIndex().get(), raftServer.getCommitIndex().get(), new LogEntry[0]);
            logger.info("Sending heartbeat request to " + address);
            Socket socket = null;
            try {
                socket = new Socket(address.split(":")[0], Integer.parseInt(address.split(":")[1]));
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(r);
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                AppendEntriesResponse response = (AppendEntriesResponse) in.readObject();
                if (response.isSuccess()) {
                    logger.info("Successful heartbeat sent to " + address);
                } else {
                    logger.info("Heartbeat failed to " + address);
                }


            } catch (IOException | ClassNotFoundException e) {
                logger.error("Error when sending heartbeat to " + address + ": ", e);
            } finally {
                if (socket != null) {
                    try {
                        socket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            raftServer.getScheduler().schedule(new Heartbeat(raftServer, address), Configuration.minElectionTimeout / 2, TimeUnit.MILLISECONDS);
        }
    }
}
