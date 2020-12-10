package com.raft.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.raft.server.jobs.ElectionTimeoutChecker;
import com.raft.server.jobs.Heartbeat;
import com.raft.server.jobs.RequestVote;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class RaftServer {

    private static Logger logger = LogManager.getLogger(RaftServer.class);

    AtomicLong currentTerm = new AtomicLong(-1);
    AtomicLong votedFor = new AtomicLong(-1);
    int serverId;
    AtomicInteger votes = new AtomicInteger(0);
    AtomicLong commitIndex = new AtomicLong(0);
    AtomicLong lastApplied = new AtomicLong(0);
    static int port = 8800;
    ServerState state = ServerState.FOLLOWER;
    ConcurrentMap<String, Integer> dataMap = new ConcurrentHashMap<>();
    List<LogEntry> logEntries = new ArrayList<>();
    Map<Integer, String> servers = new HashMap<>();

    LogEntry lastLogEntry = new LogEntry(-1, null);

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    private ScheduledExecutorService scheduler;

    public int getElectionTimeout() {
        return electionTimeout;
    }

    public void setElectionTimeout(int electionTimeout) {
        this.electionTimeout = electionTimeout;
    }

    private int electionTimeout = 150;

    public Random getGenerator() {
        return generator;
    }

    private Random generator;

    public long getLastHeardFromLeader() {
        return lastHeardFromLeader;
    }

    public void setLastHeardFromLeader(long lastHeardFromLeader) {
        this.lastHeardFromLeader = lastHeardFromLeader;
    }

    private long lastHeardFromLeader;

    public RaftServer(String[] args) {
        String[] serverStrings = args[0].strip().split(",");
        serverId = Integer.parseInt(args[1]);
        logger.info("I have server ID: " + serverId);
        port = Integer.parseInt(serverStrings[(int) serverId].split(":")[1]);
        for (int i = 0; i < serverStrings.length; i++) {
            if (i != serverId) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Server ID: " + i + " is at address " + serverStrings[i]);
                    servers.put(i, serverStrings[i]);
                }
            }
        }
        lastHeardFromLeader = System.currentTimeMillis();
        startServer();
    }

    private void startServer() {
        generator = new Random(serverId);
        generateNewElectionTimeout();
        if (logger.isDebugEnabled()) {
            logger.debug("Next election time out is: " + electionTimeout + "ms");
        }
        scheduler = newSingleThreadScheduledExecutor();
        scheduler.schedule(new ElectionTimeoutChecker(this), electionTimeout, TimeUnit.MILLISECONDS);

        try {
            ServerSocket socket = new ServerSocket(port);
            while (true) {
                RaftConnection raftConnection = new RaftConnection(socket.accept(), this);

                Thread thread = new Thread(raftConnection);
                thread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        RaftServer raftServer = new RaftServer(args);
        /*try {
            ServerSocket socket = new ServerSocket(port);
            while (true) {
                RaftConnection raftConnection = new RaftConnection(socket.accept(), raftServer);

                Thread thread = new Thread(raftConnection);
                thread.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }

    public long generateNewElectionTimeout() {
        electionTimeout = generator.nextInt(Configuration.maxElectionTimeout - Configuration.minElectionTimeout)
                + Configuration.minElectionTimeout;
        return electionTimeout;
    }

    public void setState(ServerState state) {
        this.state = state;
    }

    public void convertToCandidate() {
        if (votedFor.compareAndSet(-1, serverId)) {
            currentTerm.incrementAndGet();
            votes.incrementAndGet();
            state = ServerState.CANDIDATE;
        }

    }

    public void sendVoteRequests() {
        for (int key : servers.keySet()) {
            if (key != serverId) {
                RequestVote requestVote = new RequestVote(this, servers.get(key));
                Thread thread = new Thread(requestVote);
                thread.start();
            }
        }
    }

    public AtomicLong getCurrentTerm() {
        return currentTerm;
    }

    public AtomicLong getVotedFor() {
        return votedFor;
    }

    public int getServerId() {
        return serverId;
    }

    public AtomicLong getCommitIndex() {
        return commitIndex;
    }

    public AtomicLong getLastApplied() {
        return lastApplied;
    }

    public int getLastLogEntryTerm() {
        return lastLogEntry.getTerm();
    }


    public void addVoteAndCheckWin() {
        votes.incrementAndGet();
        if (state == ServerState.CANDIDATE && votes.get() > servers.keySet().size() / 2) {
            logger.info("Elected as leader");
            state = ServerState.LEADER;
            startHeartbeats();
        }
    }

    private void startHeartbeats() {
        for (int key : servers.keySet()) {
            if (key != serverId) {
                new Thread(new Heartbeat(this, servers.get(key))).start();
            }
        }
    }

    public void resetVotedFor() {
        votedFor.set(-1);
    }

    public ServerState getState() {
        return state;
    }

}
