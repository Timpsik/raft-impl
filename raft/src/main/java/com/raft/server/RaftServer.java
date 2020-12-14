package com.raft.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.raft.requests.ChangeStateRequest;
import com.raft.requests.ChangeStateResponse;
import com.raft.requests.ReadRequest;
import com.raft.requests.ReadResponse;
import com.raft.server.jobs.AppendLogEntries;
import com.raft.server.jobs.ElectionTimeoutChecker;
import com.raft.server.jobs.Heartbeat;
import com.raft.server.jobs.ReadHeartbeat;
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
    private AtomicInteger votes = new AtomicInteger(1);
    private int electionTimeout = 150;
    private AtomicLong lastHeardFromLeader = new AtomicLong();
    private Lock electionLock = new ReentrantLock();
    private Lock appendLock = new ReentrantLock();

    private int leaderId;

    private AtomicInteger commitIndex = new AtomicInteger(-1);
    private AtomicInteger lastApplied = new AtomicInteger(-1);
    private ConcurrentMap<String, Integer> dataMap = new ConcurrentHashMap<>();
    private List<LogEntry> logEntries = new ArrayList<>();

    private ConcurrentMap<Integer, Integer> nextIndices = new ConcurrentHashMap<>();
    private ConcurrentMap<Integer, Integer> matchIndices = new ConcurrentHashMap<>();

    private ConcurrentMap<Integer, Set<Long>> clientServerMap = new ConcurrentHashMap<>();

    private Map<Integer, RaftServerConnection> connections = new HashMap<>();


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
                }
                servers.put(i, serverStrings[i]);
                connections.put(i, new RaftServerConnection(i, serverStrings[i]));
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
            try {
                electionLock.lock();
                currentTerm.incrementAndGet();
                state = ServerState.CANDIDATE;
            } finally {
                electionLock.unlock();
            }
            logger.info("Starting election for term " + currentTerm.get() + ", currentTime : " + System.nanoTime() + " Last heard time: " + (getLastHeardFromLeader() + getElectionTimeout() * 1000 * 1000));
            lastHeardFromLeader.set(System.nanoTime());
        }

    }

    public void sendVoteRequests() {
        for (int key : servers.keySet()) {
            if (key != serverId) {
                RequestVote requestVote = new RequestVote(this, servers.get(key), key);
                Thread thread = new Thread(requestVote);
                thread.start();
            }
        }
    }

    public void addVoteAndCheckWin() {
        votes.incrementAndGet();
        if (state == ServerState.CANDIDATE && votes.get() > servers.keySet().size() / 2) {
            logger.info("Elected as leader for term " + currentTerm.get());
            state = ServerState.LEADER;
            startHeartbeatsAndInitializeFollowerInfo();
        }
    }

    private void startHeartbeatsAndInitializeFollowerInfo() {
        logger.info("Starting heartbeats");
        for (int key : servers.keySet()) {
            nextIndices.put(key, logEntries.size());
            matchIndices.put(key, 0);
            if (key != serverId) {
                new Thread(new Heartbeat(this, servers.get(key), key)).start();
            }
        }
    }

    public void resetVotedFor() {
        votedFor.set(-1);
    }

    public ChangeStateResponse modifyLog(ChangeStateRequest r) {
        Set<Long> servedRequests = clientServerMap.getOrDefault(r.getClientId(), new HashSet<>());
        if (servedRequests.contains(r.getRequestNr()) ) {
            return new ChangeStateResponse(getLeaderAddress(), true);
        }
        LogEntry newEntry = new LogEntry(currentTerm.get(), new StateChange(r.getVar(), Integer.toString(r.getValue())), r.getClientId(), r.getRequestNr());
        logEntries.add(newEntry);
        servedRequests.add(r.getRequestNr());
        clientServerMap.put(r.getClientId(), servedRequests);
        return sendLogUpdateAndApplyToState(newEntry);
    }

    private ChangeStateResponse sendLogUpdateAndApplyToState(LogEntry logEntry) {
        CountDownLatch countDownLatch = new CountDownLatch(servers.keySet().size() / 2);
        AtomicInteger successCounter = new AtomicInteger(1);
        for (int key : servers.keySet()) {
            if (key != serverId) {
                new Thread(new AppendLogEntries(this, servers.get(key), key, countDownLatch, successCounter)).start();
            }
        }
        try {
            countDownLatch.await();
            if (successCounter.get() > servers.keySet().size() / 2 ) {
                dataMap.put(logEntry.getChange().key, Integer.valueOf(logEntry.getChange().value));
                lastApplied.incrementAndGet();
                commitIndex.incrementAndGet();
                return new ChangeStateResponse(getLeaderAddress(), true);
            }
        } catch (InterruptedException e) {
            logger.error("sendLogUpdateAndApplyToState() - CountDownLatch: ", e);
            e.printStackTrace();
        }
        return new ChangeStateResponse(getLeaderAddress(), false);
    }

    public void resetVotes() {
        votes.set(1);
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

    public AtomicInteger getCommitIndex() {
        return commitIndex;
    }

    public AtomicInteger getLastApplied() {
        return lastApplied;
    }

    public long getLastLogEntryTerm() {
        if (logEntries.size() != 0) {
            return logEntries.get(logEntries.size() - 1).getTerm();
        }
        return 0;

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

    public String getLeaderAddress() {
        return servers.get(leaderId);
    }

    public List<LogEntry> getLogEntries() {
        return logEntries;
    }

    public int getNextIndex(int serverId) {
        return nextIndices.get(serverId);
    }

    public LogEntry[] getLogEntriesSince(int startIndex) {
        if (startIndex < logEntries.size()) {
            List<LogEntry> logEntriesSince = this.logEntries.subList(startIndex, logEntries.size());
            LogEntry[] newEntries = new LogEntry[logEntriesSince.size()];
            logEntriesSince.toArray(newEntries);
            return newEntries;
        }
        return new LogEntry[0];
    }

    public int reduceAndGetNextIndex(int serverId) {
        int nextIndex = nextIndices.get(serverId);
        nextIndex--;
        nextIndices.put(serverId, nextIndex);
        return nextIndex;
    }

    public long getLogEntryTerm(int logEntryIndex) {
        if (logEntryIndex < 0) {
            return -1;
        }
        return logEntries.get(logEntryIndex).getTerm();
    }

    public void setNextIndex(int serverId, int newNextIndex) {
        nextIndices.put(serverId, newNextIndex);
    }

    public void setMatchIndex(int serverId, int newNextIndex) {
        matchIndices.put(serverId, newNextIndex);
    }

    public void applyStateChange(StateChange change) {
        dataMap.put(change.key, Integer.valueOf(change.value));
    }

    public ReadResponse readFromState(ReadRequest r) {
        CountDownLatch countDownLatch = new CountDownLatch(servers.keySet().size() / 2);
        AtomicInteger successCounter = new AtomicInteger(1);
        for (int key : servers.keySet()) {
            if (key != serverId) {
                new Thread(new ReadHeartbeat(this, servers.get(key), key, countDownLatch, successCounter)).start();
            }
        }
        try {
            countDownLatch.await();
            if (successCounter.get() > servers.keySet().size() / 2 ) {
                Integer value = dataMap.getOrDefault(r.getVar(), null);
                return new ReadResponse(getLeaderAddress(), true, value);
            }
        } catch (InterruptedException e) {
            logger.error("sendLogUpdateAndApplyToState() - CountDownLatch: ", e);
            e.printStackTrace();
        }
        return new ReadResponse(getLeaderAddress(), false, -1);


    }

    public RaftServerConnection getServerConnection(int serverId) {
        return connections.get(serverId);
    }

    public void getElectionLock() {
        logger.info("Locking elections");
        electionLock.lock();
    }

    public void releaseElectionLock() {
        logger.info("Unlocking elections");
        electionLock.unlock();
    }

    public void getAppendLock() {
        logger.debug("Locking append");
        appendLock.lock();
    }

    public void releaseAppendLock() {
        logger.debug("Unlocking append");
        appendLock.unlock();
    }
}
