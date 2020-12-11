package com.raft.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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

import static java.util.concurrent.Executors.newScheduledThreadPool;

public class RaftServer {

    private static Logger logger = LogManager.getLogger(RaftServer.class);

    private final int serverId;
    private final int port;
    private final Random generator;
    private final ScheduledExecutorService scheduler;

    private ServerState state = ServerState.FOLLOWER;

    private Map<Integer, String> servers = new HashMap<>();

    private AtomicLong currentTerm = new AtomicLong(-1);
    private AtomicLong votedFor = new AtomicLong(-1);
    private AtomicInteger votes = new AtomicInteger(0);
    private int electionTimeout = 150;
    private AtomicLong lastHeardFromLeader = new AtomicLong();

    private int leaderId;

    private AtomicLong commitIndex = new AtomicLong(0);
    private AtomicLong lastApplied = new AtomicLong(0);
    private ConcurrentMap<String, Integer> dataMap = new ConcurrentHashMap<>();
    private List<LogEntry> logEntries = new ArrayList<>();
    private LogEntry lastLogEntry = new LogEntry(-1, null);

    public RaftServer(String[] args) {
        String[] serverStrings = args[0].strip().split(",");
        serverId = Integer.parseInt(args[1]);
        logger.info("I have server ID: " + serverId);
        port = Integer.parseInt(serverStrings[(int) serverId].split(":")[1]);
        generator = new Random(serverId);
        scheduler = newScheduledThreadPool(20);
        for (int i = 0; i < serverStrings.length; i++) {
            if (i != serverId) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Server ID: " + i + " is at address " + serverStrings[i]);
                    servers.put(i, serverStrings[i]);
                }
            }
        }
        lastHeardFromLeader.set(System.nanoTime());
        startServer();
    }

    private void startServer() {
        generateNewElectionTimeout();
        if (logger.isDebugEnabled()) {
            logger.debug("Next election time out is: " + electionTimeout + "ms");
        }
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
    }

    public long generateNewElectionTimeout() {
        electionTimeout = generator.nextInt(Configuration.maxElectionTimeout - Configuration.minElectionTimeout)
                + Configuration.minElectionTimeout;
        return electionTimeout;
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

    public void modifyLog(String var, int value) {
        sendLogUpdate(new LogEntry(currentTerm.get(), new StateChange(var, Integer.toString(value))));
        dataMap.put(var, value);
    }

    private void sendLogUpdate(LogEntry logEntry) {

    }

    public void resetVotes() {
        votes.set(0);
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

    public long getLastLogEntryTerm() {
        return lastLogEntry.getTerm();
    }

    public void setState(ServerState state) {
        this.state = state;
    }

    public Random getGenerator() {
        return generator;
    }

    public long getLastHeardFromLeader() {
        return lastHeardFromLeader.get();
    }

    public void setLastHeardFromLeader(long lastHeardFromLeader) {
        this.lastHeardFromLeader.set(lastHeardFromLeader);
    }

    public int getElectionTimeout() {
        return electionTimeout;
    }

    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public ServerState getState() {
        return state;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(int leaderId) {
        this.leaderId = leaderId;
    }
}
