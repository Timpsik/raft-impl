package com.raft.server.snapshot;

import com.raft.server.RaftServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Schedules the snapshots
 */
public class SnapshotManager {

    private static final Logger logger = LogManager.getLogger(SnapshotManager.class);
    private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
    private static final String FILE_PREFIX = "raft_snapshot";
    private static final String DIR_NAME = "snapshots";
    private static final int SNAPSHOT_TIME = 10;

    public static final String CONFIGURATION = "Configuration";
    public static final String CLIENTS = "Clients";

    /**
     * Next snapshot counter
     */
    private int counter = 0;

    /**
     * The Raft server
     */
    private final RaftServer server;

    /**
     * Last stored index in the last snapshot
     */
    private int lastStoredIndex = -1;

    /**
     * The last snapshot which was created
     */
    private Snapshot lastSnapshot = null;

    public SnapshotManager(RaftServer server) {
        this.server = server;
        checkDirectoryExists();
    }

    /**
     * Check if snapshot directory exists
     */
    private void checkDirectoryExists() {
        File file = new File(DIR_NAME);
        // Makes directory if it doesn't exist, else does nothing
        file.mkdir();
    }

    /**
     * Schedule the snapshot at fixed interval
     */
    public void start() {
        server.getScheduler().scheduleAtFixedRate(new SnapshotTask(this), SNAPSHOT_TIME, SNAPSHOT_TIME, TimeUnit.MINUTES);
    }

    /**
     * Check if server has stored new log entries since last snapshot
     *
     * @return true if new entries have been entries since last snapshot
     */
    public boolean isStateChanged() {
        return server.getLastApplied() > lastStoredIndex;
    }

    /**
     * Load server state from snapshot
     *
     * @return True if server state was restored from snapshot
     */
    public boolean loadSnapshot() {
        File folder = new File(DIR_NAME);
        File lastSnapshotFile = null;
        int lastNumber = -1;
        // Find the file with the last counter
        for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
            // File start with snapshot prefix
            if (fileEntry.getName().startsWith(FILE_PREFIX)) {
                // The index of char before counter
                int startInx = fileEntry.getName().lastIndexOf("_");
                // The index of char after counter
                int endInx = fileEntry.getName().indexOf(".");
                // Get the counter
                String counter = fileEntry.getName().substring(startInx + 1, endInx);
                // Remember if biggest counter so far
                if (Integer.parseInt(counter) > lastNumber) {
                    lastNumber = Integer.parseInt(counter);
                    lastSnapshotFile = fileEntry;
                }
            }
        }
        // Check if there exists a snapshot file
        if (lastSnapshotFile != null) {
            InputStream inputStream = null;
            try {
                inputStream = new FileInputStream(lastSnapshotFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                // Read last term in snapshot
                long lastTerm = Long.parseLong(br.readLine());
                // Read last applied index in snapshot
                int lastApplied = Integer.parseInt(br.readLine());
                String line;
                // Read the storage data from file until Configuration
                Map<String, Integer> data = new HashMap<>();
                while ((line = br.readLine()) != null && !line.startsWith(CONFIGURATION)) {
                    data.put(line.split("\t")[0], Integer.parseInt(line.split("\t")[1]));
                }

                // Read the servers addresses from file until Clients
                Map<Integer, String> servers = new HashMap<>();
                while ((line = br.readLine()) != null && !line.startsWith(CLIENTS)) {
                    if (!"".equals(line.trim())) {
                        servers.put(Integer.parseInt(line.split("\t")[0]), line.split("\t")[1]);
                    }
                }
                // Read the served clients from file
                Map<Integer, Long> servedClients = new HashMap<>();
                while ((line = br.readLine()) != null) {
                    if (!"".equals(line.trim())) {
                        servedClients.put(Integer.parseInt(line.split("\t")[0]), Long.parseLong(line.split("\t")[1]));
                    }
                }

                // Set the server state
                server.setMachineState(data);
                server.setServersState(servers);
                server.setTermOfLastApplied(lastTerm);
                server.setCurrentTerm(lastTerm);
                server.setLastApplied(lastApplied);
                server.setCommitIndex(lastApplied);
                server.setNextIndex(lastApplied + 1);
                server.setServedClients(servedClients);
                lastStoredIndex = lastApplied;
                lastSnapshot = new Snapshot(lastApplied, lastTerm, data, servers, servedClients);
                // Set the counter to the snapshot file counter incremented by one
                counter = lastNumber + 1;
                logger.info("Loaded snapshot");
                return true;
            } catch (Exception e) {
                logger.error("Error when reading snapshot: ", e);
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException ex) {
                        logger.warn("Error when closing input stream: ", ex);
                    }
                }
            }
        }
        return false;
    }

    /**
     * Get the file name of the next snapshot file
     *
     * @return name of the next snapshot file
     */
    public String getFileName() {
        String fileName = DIR_NAME + File.separator + FILE_PREFIX + dtf.format(LocalDateTime.now()) + "_" + counter + ".txt";
        //Increase snapshot counter
        counter++;
        return fileName;
    }

    public void setLastStoredIndex(int lastStoredIndex) {
        this.lastStoredIndex = lastStoredIndex;
    }

    public void setLastSnapshot(Snapshot snapshot) {
        lastSnapshot = snapshot;
        lastStoredIndex = snapshot.getLastLogIndex();
    }

    public RaftServer getServer() {
        return server;
    }

    public Snapshot getLastSnapshot() {
        return lastSnapshot;
    }

    public int getLastStoredIndex() {
        return lastStoredIndex;
    }
}