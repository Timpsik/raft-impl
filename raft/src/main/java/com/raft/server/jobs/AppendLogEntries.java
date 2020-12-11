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

public class AppendLogEntries implements Runnable {


    private static Logger logger = LogManager.getLogger(AppendLogEntries.class);

    private final RaftServer raftServer;
    private final String address;
    private LogEntry logEntry;

    public AppendLogEntries(RaftServer raftServer, String address, LogEntry logEntry) {
        this.raftServer = raftServer;
        this.address = address;
        this.logEntry = logEntry;
    }

    @Override
    public void run() {
        if (raftServer.getState() == ServerState.LEADER) {
            LogEntry[] entries = new LogEntry[1];
            entries[0] = logEntry;
            AppendEntriesRequest r = new AppendEntriesRequest(raftServer.getCurrentTerm().get(), raftServer.getServerId(),
                    raftServer.getLastLogEntryTerm(), raftServer.getCommitIndex().get(), raftServer.getCommitIndex().get(), entries);
            logger.info("Sending log entries request to " + address);
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
