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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.raft.requests.AckResponse;
import com.raft.requests.AddServerRequest;
import com.raft.requests.ChangeStateRequest;
import com.raft.requests.ErrorCause;
import com.raft.requests.ReadRequest;
import com.raft.requests.ReadResponse;
import com.raft.requests.RemoveServerRequest;
import com.raft.server.conf.Configuration;
import com.raft.server.conf.ServerState;
import com.raft.server.connection.IncomingConnection;
import com.raft.server.connection.ServerOutConnection;
import com.raft.server.entries.AddRaftServerChange;
import com.raft.server.entries.Change;
import com.raft.server.entries.LogEntry;
import com.raft.server.entries.RemoveRaftServerChange;
import com.raft.server.entries.StateChange;
import com.raft.server.jobs.AppendLogEntries;
import com.raft.server.jobs.ElectionTimeoutChecker;
import com.raft.server.jobs.Heartbeat;
import com.raft.server.jobs.RequestVote;
import com.raft.server.rpc.ServerRequest;
import com.raft.server.snapshot.Snapshot;
import com.raft.server.snapshot.SnapshotManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.util.concurrent.Executors.newScheduledThreadPool;

public class RaftServer {

    private static Logger logger = LogManager.getLogger(RaftServer.class);

    /**
     * Server id
     */
    private final int serverId;

    /**
     * Port of the server socket
     */
    private final int port;
    private final Random generator;

    /**
     * Scheduler of periodic tasks
     */
    private final ScheduledExecutorService scheduler;

    /**
     * State of the server, start as a follower
     */
    private ServerState state = ServerState.FOLLOWER;

    /**
     * Map, key is the id and value is the address of the server
     */
    private Map<Integer, String> servers = new HashMap<>();

    /**
     * Term of the server
     */
    private AtomicLong currentTerm = new AtomicLong(-1);

    /**
     * Id of the server that received vote for term
     */
    private AtomicLong votedFor = new AtomicLong(-1);

    /**
     * Votes for server, each server has one vote from itself if it converts to candidate.
     */
    private AtomicInteger votes = new AtomicInteger(1);

    /**
     * Election timeout to start new election in ms
     */
    private int electionTimeout = 150;

    /**
     * Last time heard from leader in nano seconds
     */
    private AtomicLong lastHeardFromLeader = new AtomicLong();

    /**
     * Lock for using term in election situations
     */
    private Lock electionLock = new ReentrantLock();

    /**
     * Lock for appending entries to log
     */
    private Lock appendLock = new ReentrantLock();

    /**
     * Id of the current leader
     */
    private int leaderId;

    /**
     * Last commited index to the state
     */
    private AtomicInteger commitIndex = new AtomicInteger(-1);

    /**
     *  Last log entry added to the log
     */
    private AtomicInteger lastApplied = new AtomicInteger(-1);

    /**
     * Index of the next new entry
     */
    private int nextIndex = 0;

    /**
     * Term of the last applied index
     */
    private long termOfLastApplied = -1;

    /**
     * Storage, keys are variable names and values are integers
     */
    private ConcurrentMap<String, Integer> dataMap = new ConcurrentHashMap<>();

    /**
     * Log entry list
     */
    private List<LogEntry> logEntries = new ArrayList<>();

    /**
     * Next indices for all followers
     */
    private ConcurrentMap<Integer, Integer> nextIndices = new ConcurrentHashMap<>();

    /**
     * Matching log index for all followers
     */
    private ConcurrentMap<Integer, Integer> matchIndices = new ConcurrentHashMap<>();

    /**
     * Map for storing client  and its last request
     */
    private ConcurrentMap<Integer, Long> clientServerMap = new ConcurrentHashMap<>();

    /**
     * Connections between servers
     */
    private Map<Integer, ServerOutConnection> connections = new HashMap<>();

    /**
     * Manages snapshots
     */
    private final SnapshotManager snapshotManager;

    public RaftServer(String[] args) {
        logger.info("Given arguments: " + args[0] + " " + args[1]);
        // Split the server addresses
        String[] serverStrings = args[0].split(",");
        // Get the id of the server
        serverId = Integer.parseInt(args[1]);
        logger.info("I have server ID: " + serverId);
        // Get the port
        port = Integer.parseInt(serverStrings[serverId].split(":")[1]);
        generator = new Random(serverId);
        scheduler = newScheduledThreadPool(20);
        snapshotManager = new SnapshotManager(this);
        // Try to load snapshot if there exists one
        if (snapshotManager.loadSnapshot()) {
            for (int id : servers.keySet()) {
                if (id != serverId) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Server ID: " + id + " is at address " + serverStrings[id]);
                    }
                    connections.put(id, new ServerOutConnection(serverStrings[id]));
                }
            }
        } else {
            // Store given servers and try to create connections to them
            for (int i = 0; i < serverStrings.length; i++) {
                if (i != serverId) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Server ID: " + i + " is at address " + serverStrings[i]);
                    }
                    connections.put(i, new ServerOutConnection(serverStrings[i]));
                }
                servers.put(i, serverStrings[i]);
            }
        }
        // Initialize last heard from leader
        lastHeardFromLeader.set(System.nanoTime());
        startServer();
    }

    private void startServer() {
        // Generate next election timeout
        generateNewElectionTimeout();
        if (logger.isDebugEnabled()) {
            logger.debug("Next election time out is: " + electionTimeout + "ms");
        }
        // Schedule election timeout checker
        scheduler.schedule(new ElectionTimeoutChecker(this), electionTimeout, TimeUnit.MILLISECONDS);
        snapshotManager.start();

        try {
            // Create socket to listen to new connections
            ServerSocket socket = new ServerSocket(port);
            while (true) {
                // Accept incoming connections
                IncomingConnection incomingConnection = new IncomingConnection(socket.accept(), this);
                Thread thread = new Thread(incomingConnection);
                thread.start();
            }
        } catch (IOException e) {
            logger.warn("Exception in accepting new connection: ", e);
        }
    }

    public static void main(String[] args) {
        RaftServer raftServer = new RaftServer(args);
    }

    /**
     * Generate new election timeout
     * @return new time in ms to check if the leader is alive
     */
    public long generateNewElectionTimeout() {
        electionTimeout = generator.nextInt(Configuration.maxElectionTimeout - Configuration.minElectionTimeout)
                + Configuration.minElectionTimeout;
        return electionTimeout;
    }

    /**
     * Convert to candidate state
     */
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

    /**
     * Send vote requests to other servers
     */
    public void sendVoteRequests() {
        for (int key : servers.keySet()) {
            if (key != serverId) {
                RequestVote requestVote = new RequestVote(this, servers.get(key), key);
                Thread thread = new Thread(requestVote);
                thread.start();
            }
        }
    }

    /**
     * Add one received win and check if enough votes to become leader
     */
    public void addVoteAndCheckWin() {
        votes.incrementAndGet();
        try {
            electionLock.lock();
            if (state == ServerState.CANDIDATE && votes.get() > servers.keySet().size() / 2) {
                logger.warn("Elected as leader for term " + currentTerm.get() + " at " + System.currentTimeMillis());
                state = ServerState.LEADER;
                startHeartbeatsAndInitializeFollowerInfo();
            }
        } finally {
            electionLock.unlock();
        }

    }

    private void startHeartbeatsAndInitializeFollowerInfo() {
        logger.warn("Starting heartbeats");
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

    /**
     *  Add new log entry
     * @param r request from client
     * @return
     */
    public AckResponse modifyLog(ChangeStateRequest r) {
        Long lastServedRequests = clientServerMap.getOrDefault(r.getClientId(), null);
        // Check if request is already serverd
        if (lastServedRequests != null && lastServedRequests > r.getRequestNr()) {
            return new AckResponse(getLeaderAddress(), false, ErrorCause.ALREADY_PROCESSED);
        }
        LogEntry newEntry;
        try {
            appendLock.lock();
            newEntry = new LogEntry(currentTerm.get(), new StateChange(r.getVar(), Integer.toString(r.getValue())), r.getClientId(), r.getRequestNr(), nextIndex);
            nextIndex++;
            logEntries.add(newEntry);
            clientServerMap.put(r.getClientId(), r.getRequestNr());
        } finally {
            appendLock.unlock();
        }
        return sendLogUpdateAndApplyToState(newEntry);
    }

    /**
     * Send the log update to other servers
     * @param logEntry
     * @return
     */
    private AckResponse sendLogUpdateAndApplyToState(LogEntry logEntry) {
        CountDownLatch countDownLatch = new CountDownLatch(servers.keySet().size() / 2);
        AtomicInteger successCounter = new AtomicInteger(1);
        for (int key : servers.keySet()) {
            if (key != serverId) {
                new Thread(new AppendLogEntries(this, servers.get(key), key, countDownLatch, successCounter)).start();
            }
        }
        try {
            // Wait for answers from half of the cluster
            countDownLatch.await();
            // If the requests were succesful commit the change
            if (successCounter.get() > servers.keySet().size() / 2) {
                try {
                    appendLock.lock();
                    Change change = logEntry.getChange();
                    if (change instanceof StateChange) {
                        StateChange stateChange = (StateChange) change;
                        dataMap.put(stateChange.getKey(), Integer.valueOf(stateChange.getValue()));
                    } else if (change instanceof AddRaftServerChange) {

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
        }
        return new AckResponse(getLeaderAddress(), false, ErrorCause.NOT_LEADER);
    }

    public void resetVotes() {
        votes.set(1);
    }

    public long getCurrentTerm() {
        return currentTerm.get();
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm.set(currentTerm);
    }

    public AtomicLong getVotedFor() {
        return votedFor;
    }

    public int getServerId() {
        return serverId;
    }

    public int getCommitIndex() {
        return commitIndex.get();
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex.set(commitIndex);
    }

    public int getLastApplied() {
        return lastApplied.get();
    }

    public void setLastApplied(int lastApplied) {
        this.lastApplied.set(lastApplied);
    }

    /**
     * Get the term of the last entry in log
     * @return
     */
    public long getLastLogEntryTerm() {
        if (logEntries.size() != 0) {
            return logEntries.get(logEntries.size() - 1).getTerm();
        }
        // If  there are no logs in list try to check what is the last included log in the snapshot
        if (snapshotManager.getLastSnapshot() != null) {
            return snapshotManager.getLastSnapshot().getLastTerm();
        }
        return -1;
    }

    public void setState(ServerState state) {
        this.state = state;
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

    /**
     * Get all log indices from given index
     * @param startIndex index to get all indices since
     * @return
     */
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

    /**
     * Reduce the next index for given server
     * @param serverId server id of the server for which to reduce the server id
     * @return
     */
    public int reduceAndGetNextIndex(int serverId) {
        int nextIndex = nextIndices.get(serverId);
        nextIndex--;
        nextIndices.put(serverId, nextIndex);
        return nextIndex;
    }

    /**
     * Get Term of the log entry at give index
     * @param logEntryIndex index of the log entry for which term is needed
     * @return
     */
    public long getLogEntryTerm(int logEntryIndex) {
        // Check if index is in log
        if (logEntries.size() != 0 && logEntryIndex >= logEntries.get(0).getIndex()) {
            for (int i = 0; i < logEntries.size(); i++) {
                if (logEntries.get(i).getIndex() == logEntryIndex) {
                    return logEntries.get(i).getTerm();
                }
            }
        } else if (snapshotManager.getLastSnapshot() != null && snapshotManager.getLastStoredIndex() == logEntryIndex) {
            // If it is the last entry in snapshot use the snapshot last included term
            logger.info("Taking snapshot term: " + snapshotManager.getLastSnapshot().getLastTerm() + "for entry " + logEntryIndex);
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

    /**
     * Commit the state change
     */
    public void applyStateChange(Change change) {
        if (change instanceof StateChange) {
            dataMap.put(((StateChange) change).getKey(), Integer.valueOf(((StateChange) change).getValue()));
        }
    }

    /**
     * Handle the read request from client
     * @param r read request
     * @return response to the client
     */
    public ReadResponse readFromState(ReadRequest r) {
        CountDownLatch countDownLatch = new CountDownLatch(servers.keySet().size() / 2);
        AtomicInteger successCounter = new AtomicInteger(1);
        // Send heartbeats
        for (int key : servers.keySet()) {
            if (key != serverId) {
                new Thread(new AppendLogEntries(this, servers.get(key), key, countDownLatch, successCounter)).start();
            }
        }
        try {
            // Wait for responses from other followers
            countDownLatch.await();
            // If majority of heartbeats was successful, respond to client
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
        return new ReadResponse(getLeaderAddress(), false, ErrorCause.REQUEST_FAILED);


    }

    public ServerOutConnection getServerConnection(int serverId) {
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

    /**
     * Add new server to the configuration
     * @param r
     * @return
     */
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

    /**
     * Update the last served request for client
     * @param clientId id of the client
     * @param requestNr new request number for client
     */
    public void addServedRequest(int clientId, long requestNr) {
        clientServerMap.put(clientId, requestNr);
    }

    public Map<Integer, String> getServers() {
        return servers;
    }

    /**
     * Increment the next index for new entry
     */
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

    /**
     * Remove all logs until the given index
     * @param lastLogIndex
     */
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

    /**
     * Remove server from configuration
     * @param r request sent by the client
     * @return
     */
    public AckResponse removeServer(RemoveServerRequest r) {
        Long lastServedRequests = clientServerMap.getOrDefault(r.getClientId(), null);
        if (lastServedRequests != null && lastServedRequests > r.getRequestNr()) {
            return new AckResponse(getLeaderAddress(), true);
        }

        clientServerMap.put(r.getClientId(), r.getRequestNr());
        //TODO:: Remove server from configuration
        return new AckResponse(getLeaderAddress(), true);
    }

    /**
     * Get the term of last applied entry
     * @return
     */
    public long getLastAppliedTerm() {
        // Get it from log
        if (logEntries.size() != 0 && lastApplied.get() > logEntries.get(0).getIndex()) {
            for (int i = 0; i < logEntries.size(); i++) {
                if (logEntries.get(i).getIndex() == lastApplied.get()) {
                    return logEntries.get(i).getTerm();
                }
            }
        } else if (snapshotManager.getLastSnapshot() != null) {
            // Get it from snapshot
            return snapshotManager.getLastSnapshot().getLastTerm();
        }
        return -1;
    }


    public void setLastSnapshot(Snapshot snapshot) {
        snapshotManager.setLastSnapshot(snapshot);
    }

    /**
     *  Clear log after given entry
     * @param entry
     */
    public void clearLogFromEntry(LogEntry entry) {
        for (int i = 0; i < logEntries.size(); i++) {
            if (logEntries.get(i).getIndex() == entry.getIndex()) {
                logEntries.subList(i, logEntries.size()).clear();
                nextIndex = entry.getIndex() + 1;
                break;
            }
        }
    }

    /**
     * Get log entry at given index
     * @param commitInx
     * @return
     */
    public LogEntry getLogEntry(int commitInx) {
        if (commitInx <= snapshotManager.getLastStoredIndex()) {
            return null;
        }
        for (int i = 0; i < logEntries.size(); i++) {
            if (logEntries.get(i).getIndex() == commitInx) {
                return logEntries.get(i);
            }
        }
        return null;
    }

    public Map<Integer, Long> getServedClients() {
        return clientServerMap;
    }

    public void setServedClients(Map<Integer, Long> servedClients) {
        clientServerMap.clear();
        clientServerMap.putAll(servedClients);
    }

    /**
     * Commit the entry at given index
     * @param indexToCommit
     */
    public void commitEntry(int indexToCommit) {
        LogEntry entry = getLogEntry(indexToCommit);
        if (entry != null) {
            applyStateChange(entry.getChange());
            setCommitIndex(indexToCommit);
            setLastApplied(indexToCommit);
            setTermOfLastApplied(entry.getTerm());
        } else {
            logger.warn("Entry not in log");
        }
    }

    /**
     * Convert to follower state
     * @param request
     */
    public void convertToFollower(ServerRequest request) {
        setCurrentTerm(request.getTerm());
        setState(ServerState.FOLLOWER);
        setLeaderId(request.getSenderId());
        setLastHeardFromLeader(System.nanoTime());
    }
}
