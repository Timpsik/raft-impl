package com.raft.server.snapshot;

import com.raft.server.RaftServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class SnapshotManager {

    private static Logger logger = LogManager.getLogger(SnapshotManager.class);

    private int counter = 0;
    private final RaftServer server;
    private final String FILE_PREFIX = "raft_snapshot";
    private final String DIR_NAME = "snapshots";
    private int lastStoredIndex = -1;
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");
    private Snapshot lastSnapshot = null;
    public static final String CONFIGURATION = "Configuration";

    public SnapshotManager(RaftServer server) {
        this.server = server;
        checkDirectoryExists();
    }

    private void checkDirectoryExists() {
        File file = new File(DIR_NAME);
        file.mkdir();
    }


    public void start() {
        server.getScheduler().scheduleAtFixedRate(new SnapshotTask(this), 1, 1, TimeUnit.MINUTES);
    }

    public int getCounter() {
        return counter;
    }

    public int getCounterAndIncrement() {
        int returnValue = counter;
        counter++;
        return returnValue;
    }

    public boolean isStateChanged() {
        return server.getLastApplied().get() > lastStoredIndex;
    }

    public RaftServer getServer() {
        return server;
    }

    public String getFileName() {
        String fileName = DIR_NAME + File.separator + FILE_PREFIX + dtf.format(LocalDateTime.now()) + "_" + counter + ".txt";
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

    public Snapshot getLastSnapshot() {
        return lastSnapshot;
    }

    public int getLastStoredIndex() {
        return lastStoredIndex;
    }

    public boolean loadSnapshot() {
        File folder = new File(DIR_NAME);
        File lastSnapshotFile = null;
        int lastNumber = -1;
        for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
            if (fileEntry.getName().startsWith(FILE_PREFIX)) {
                int startInx = fileEntry.getName().lastIndexOf("_");
                int endInx = fileEntry.getName().indexOf(".");
                String counter = fileEntry.getName().substring(startInx + 1, endInx);
                if (Integer.parseInt(counter) > lastNumber) {
                    lastNumber = Integer.parseInt(counter);
                    lastSnapshotFile = fileEntry;
                }
            }
        }
        if (lastSnapshotFile != null) {
            InputStream inputStream = null;
            try {
                inputStream = new FileInputStream(lastSnapshotFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                long lastTerm = Long.parseLong(br.readLine());
                int lastApplied = Integer.parseInt(br.readLine());
                String line;
                Map<String, Integer> data = new HashMap<>();
                while (!CONFIGURATION.equals(line = br.readLine().strip())) {
                    data.put(line.split("\t")[0], Integer.parseInt(line.split("\t")[1]));
                }

                Map<Integer, String> servers = new HashMap<>();
                while ((line = br.readLine()) != null) {
                    if(!"".equals(line)){
                        servers.put(Integer.parseInt(line.split("\t")[0]), line.split("\t")[1]);
                    }
                }
                server.setMachineState(data);
                server.setServersState(servers);
                server.setTermOfLastApplied(lastTerm);
                server.getCurrentTerm().set(lastTerm);
                server.getLastApplied().set(lastApplied);
                server.getCommitIndex().set(lastApplied);
                server.setNextIndex(lastApplied+1);
                lastStoredIndex = lastApplied;
                lastSnapshot = new Snapshot(lastApplied, lastTerm, data, servers);
                counter = lastNumber + 1;
                return true;
            } catch (IOException e) {
                logger.error("Error when reading snapshot: ", e);
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
}