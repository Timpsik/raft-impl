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

import com.raft.requests.*;
import com.raft.server.conf.Configuration;
import com.raft.server.conf.ServerState;
import com.raft.server.connection.RaftConnection;
import com.raft.server.connection.RaftServerConnection;
import com.raft.server.entries.*;
import com.raft.server.jobs.AppendLogEntries;
import com.raft.server.jobs.ElectionTimeoutChecker;
import com.raft.server.jobs.Heartbeat;
import com.raft.server.jobs.ReadHeartbeat;
import com.raft.server.jobs.RequestVote;
import com.raft.server.snapshot.Snapshot;
import com.raft.server.snapshot.SnapshotManager;
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
    private int nextIndex = 0;
    private long termOfLastApplied = -1;
    private ConcurrentMap<String, Integer> dataMap = new ConcurrentHashMap<>();
    private List<LogEntry> logEntries = new ArrayList<>();

    private ConcurrentMap<Integer, Integer> nextIndices = new ConcurrentHashMap<>();
    private ConcurrentMap<Integer, Integer> matchIndices = new ConcurrentHashMap<>();

    private ConcurrentMap<Integer, Long> clientServerMap = new ConcurrentHashMap<>();

    private Map<Integer, RaftServerConnection> connections = new HashMap<>();

    private final SnapshotManager snapshotManager;

    public RaftServer(String[] args) {
        String[] serverStrings = args[0].strip().split(",");
        serverId = Integer.parseInt(args[1]);
        logger.info("I have server ID: " + serverId);
        port = Integer.parseInt(serverStrings[(int) serverId].split(":")[1]);
        generator = new Random(serverId);
        scheduler = newScheduledThreadPool(20);
        snapshotManager = new SnapshotManager(this);
        if (snapshotManager.loadSnapshot() ) {
            for (int id : servers.keySet()) {
                if (id != serverId) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Server ID: " + id + " is at address " + serverStrings[id]);
                    }
                    connections.put(id, new RaftServerConnection(id, serverStrings[id]));
                }
            }
        } else {
            for (int i = 0; i < serverStrings.length; i++) {
                if (i != serverId) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Server ID: " + i + " is at address " + serverStrings[i]);
                    }
                    connections.put(i, new RaftServerConnection(i, serverStrings[i]));
                }
                servers.put(i, serverStrings[i]);
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
        snapshotManager.start();
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
        try {
            electionLock.lock();
            if (state == ServerState.CANDIDATE && votes.get() > servers.keySet().size() / 2) {
                logger.info("Elected as leader for term " + currentTerm.get());
                state = ServerState.LEADER;
                startHeartbeatsAndInitializeFollowerInfo();
            }
        } finally {
            electionLock.unlock();
        }

    }

    private void startHeartbeatsAndInitializeFollowerInfo() {
        logger.info("Starting heartbeats");
        for (int key : servers.keySet()) {
            nextIndices.put(key, nextIndex);
            matchIndices.put(key, 0);
            if (key != serverId) {
                new Thread(new Heartbeat(this, servers.get(key), key)).start();
            }
        }
    }

    public void resetVotedFor() {
        votedFor.set(-1);
    }

    public AckResponse modifyLog(ChangeStateRequest r) {
        Long lastServedRequests = clientServerMap.getOrDefault(r.getClientId(), null);
        if (lastServedRequests != null && lastServedRequests > r.getRequestNr()) {
            return new AckResponse(getLeaderAddress(), true);
        }
        LogEntry newEntry = new LogEntry(currentTerm.get(), new StateChange(r.getVar(), Integer.toString(r.getValue())), r.getClientId(), r.getRequestNr(), nextIndex);
        nextIndex++;
        logEntries.add(newEntry);
        clientServerMap.put(r.getClientId(), r.getRequestNr());
        return sendLogUpdateAndApplyToState(newEntry);
    }

    private AckResponse sendLogUpdateAndApplyToState(LogEntry logEntry) {
        CountDownLatch countDownLatch = new CountDownLatch(servers.keySet().size() / 2);
        AtomicInteger successCounter = new AtomicInteger(1);
        for (int key : servers.keySet()) {
            if (key != serverId) {
                new Thread(new AppendLogEntries(this, servers.get(key), key, countDownLatch, successCounter)).start();
            }
        }
        try {
            countDownLatch.await();
            if (successCounter.get() > servers.keySet().size() / 2) {
                try {
                    appendLock.lock();
                    Change change = logEntry.getChange();
                    if (change instanceof StateChange) {
                        StateChange stateChange = (StateChange) change;
                        dataMap.put(stateChange.getKey(), Integer.valueOf(stateChange.getValue()));
                    } else if (change instanceof AddRaftServerChange) {
                        AddRaftServerChange stateChange = (AddRaftServerChange) change;
                        servers.put(stateChange.getServerId(), stateChange.getServerAddress());
                    } else if (change instanceof RemoveRaftServerChange) {

                    }
                    lastApplied.incrementAndGet();
                    termOfLastApplied = currentTerm.get();
                    commitIndex.incrementAndGet();
                    return new AckResponse(getLeaderAddress(), true);
                } finally {
                    appendLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            logger.error("sendLogUpdateAndApplyToState() - CountDownLatch: ", e);
            e.printStackTrace();
        }
        return new AckResponse(getLeaderAddress(), false);
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
        if (snapshotManager.getLastSnapshot() != null) {
            return snapshotManager.getLastSnapshot().getLastTerm();
        }
        return -1;
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

    public int getNextIndex() {
        return nextIndex;
    }

    public LogEntry[] getLogEntriesSince(int startIndex) {
        if (startIndex > snapshotManager.getLastStoredIndex()) {
            for (int i = 0; i < logEntries.size(); i++) {
                if (logEntries.get(i).getIndex() == startIndex) {
                    List<LogEntry> logEntriesSince = this.logEntries.subList(i, logEntries.size());
                    LogEntry[] newEntries = new LogEntry[logEntriesSince.size()];
                    logEntriesSince.toArray(newEntries);
                    return newEntries;
                }
            }
        } else {
            return null;
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
        if (logEntries.size() != 0 && logEntryIndex >= logEntries.get(0).getIndex()) {
            for (int i = 0; i < logEntries.size(); i++) {
                if (logEntries.get(i).getIndex() == logEntryIndex) {
                    return logEntries.get(i).getTerm();
                }
            }
        } else if (snapshotManager.getLastSnapshot() != null && snapshotManager.getLastStoredIndex() == logEntryIndex) {
            logger.info("Taking snapshot term");
            return snapshotManager.getLastSnapshot().getLastTerm();
        }
        return -1;
    }

    public void setNextIndex(int serverId, int newNextIndex) {
        nextIndices.put(serverId, newNextIndex);
    }

    public void setMatchIndex(int serverId, int newNextIndex) {
        matchIndices.put(serverId, newNextIndex);
    }

    public void applyStateChange(Change change) {
        if (change instanceof StateChange) {
            dataMap.put(((StateChange) change).getKey(), Integer.valueOf(((StateChange) change).getValue()));
        }
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
            if (successCounter.get() > servers.keySet().size() / 2) {
                Integer value = dataMap.getOrDefault(r.getVar(), null);
                if (value == null) {
                    return new ReadResponse(getLeaderAddress(), false, ErrorCause.ENTRY_NOT_FOUND);
                }
                return new ReadResponse(getLeaderAddress(), true, value);
            }
        } catch (InterruptedException e) {
            logger.error("sendLogUpdateAndApplyToState() - CountDownLatch: ", e);
        }
        return new ReadResponse(getLeaderAddress(), false, -1);


    }

    public RaftServerConnection getServerConnection(int serverId) {
        return connections.get(serverId);
    }

    public void getElectionLock() {
        logger.debug("Locking elections");
        electionLock.lock();
    }

    public void releaseElectionLock() {
        logger.debug("Unlocking elections");
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

    public AckResponse addNewServer(AddServerRequest r) {
        Long lastServedRequests = clientServerMap.getOrDefault(r.getClientId(), null);
        if (lastServedRequests != null && lastServedRequests > r.getRequestNr()) {
            return new AckResponse(getLeaderAddress(), true);
        }
        //TODO:: Add new server to the configuration
        return new AckResponse(getLeaderAddress(), true);
    }

    public long getTermOfLastApplied() {
        return termOfLastApplied;
    }

    public void setTermOfLastApplied(long termOfLastApplied) {
        this.termOfLastApplied = termOfLastApplied;
    }

    public Map<String, Integer> getCurrentMachineState() {
        return dataMap;
    }

    public void cleanLogUntil(int lastApplied) {
        if (lastApplied + 1 == logEntries.size()) {
            logEntries.clear();
        } else {
            logEntries = logEntries.subList(lastApplied + 1, logEntries.size());
        }
    }

    public void addServedRequest(int clientId, long requestNr) {
        clientServerMap.put(clientId, requestNr);
    }

    public Map<Integer, String> getServers() {
        return servers;
    }

    public void incrementNextIndex() {
        nextIndex++;
    }

    public Snapshot getLastSnapshot() {
        return snapshotManager.getLastSnapshot();
    }

    public void setMachineState(Map<String, Integer> state) {
        dataMap.clear();
        dataMap.putAll(state);
    }

    public void setServersState(Map<Integer, String> serversConfiguration) {
        servers.clear();
        servers.putAll(serversConfiguration);
    }

    public void setNextIndex(int nextIndex) {
        this.nextIndex = nextIndex;
    }

    public void removeEntriesUntil(int lastLogIndex) {
        for (int i = 0; i < logEntries.size(); i++) {
            if (logEntries.get(i).getIndex() > lastLogIndex) {
                return;
            } else if (logEntries.get(i).getIndex() == lastLogIndex) {
                logEntries.subList(0, i + 1).clear();
                return;
            }
        }
    }

    public AckResponse removeServer(RemoveServerRequest r) {
        Long lastServedRequests = clientServerMap.getOrDefault(r.getClientId(), null);
        if (lastServedRequests != null && lastServedRequests > r.getRequestNr()) {
            return new AckResponse(getLeaderAddress(), true);
        }

        clientServerMap.put(r.getClientId(), r.getRequestNr());
        //TODO:: Remove server from configuration
        return new AckResponse(getLeaderAddress(), true);
    }

    public long getLastAppliedTerm() {
        if (logEntries.size() != 0 && lastApplied.get() > logEntries.get(0).getIndex()) {
            for (int i = 0; i < logEntries.size(); i++) {
                if (logEntries.get(i).getIndex() == lastApplied.get()) {
                    return logEntries.get(i).getTerm();
                }
            }
        } else if (snapshotManager.getLastSnapshot() != null) {
            return snapshotManager.getLastSnapshot().getLastTerm();
        }
        return -1;
    }

    public long getTermForEntry(int entryIndex) {
        if (entryIndex < snapshotManager.getLastStoredIndex()) {
            return -1;
        } else if(snapshotManager.getLastSnapshot() != null && entryIndex == snapshotManager.getLastStoredIndex()) {
            return snapshotManager.getLastSnapshot().getLastTerm();
        } else {
            for (int i = 0; i < logEntries.size(); i++) {
                if (logEntries.get(i).getIndex() == entryIndex) {
                    return logEntries.get(i).getTerm();
                }
            }
        }
        return -1;
    }

    public void setLastSnapshot(Snapshot snapshot) {
        snapshotManager.setLastSnapshot(snapshot);
    }

    public void clearLogFromEntry(LogEntry entry) {
        for (int i = 0; i < logEntries.size(); i++) {
            if (logEntries.get(i).getIndex() == entry.getIndex()){
                logEntries.subList(i,logEntries.size()).clear();
                nextIndex = entry.getIndex() + 1;
                break;
            }
        }
    }

    public LogEntry getLogEntry(int commitInx) {
        if (commitInx <= snapshotManager.getLastStoredIndex()) {
            return null;
        }
        for (int i = 0; i < logEntries.size(); i++){
            if (logEntries.get(i).getIndex() == commitInx) {
                return logEntries.get(i);
            }
        }
        return null;
    }
}
