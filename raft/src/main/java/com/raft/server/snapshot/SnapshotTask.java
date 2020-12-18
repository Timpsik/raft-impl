package com.raft.server.snapshot;

import com.raft.server.RaftServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Snapshot creating task
 */
public class SnapshotTask implements Runnable {
    private static Logger logger = LogManager.getLogger(SnapshotTask.class);

    private final SnapshotManager manager;

    public SnapshotTask(SnapshotManager manager) {
        this.manager = manager;
    }


    @Override
    public void run() {
        logger.info("Running snapshot");
        // Check if state has changed based on the last stored log entry index
        if (manager.isStateChanged()) {
            RaftServer server = manager.getServer();
            int lastApplied;
            long termOfLastApplied;
            Map<String, Integer> storage = new HashMap<>();
            Map<Integer, String> servers = new HashMap<>();
            Map<Integer, Long> servedClients = new HashMap<>();

            try {
                // Get the lock to avoid modifications to state
                server.getAppendLock();
                lastApplied = server.getLastApplied();
                termOfLastApplied = server.getTermOfLastApplied();
                storage.putAll(server.getCurrentMachineState());
                servers.putAll(server.getServers());
                servedClients.putAll(server.getServedClients());
            } finally {
                server.releaseAppendLock();
            }

            FileOutputStream fos = null;
            BufferedWriter bw = null;
            try {
                // Get the snapshot file name
                File file = new File(manager.getFileName());
                fos = new FileOutputStream(file);
                // Write last term and last applied into file
                bw = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8.name()));
                bw.write(Long.toString(termOfLastApplied));
                bw.newLine();
                bw.write(Integer.toString(lastApplied));
                bw.newLine();
                // Write storage into file
                for (String key : storage.keySet()) {
                    bw.write(key + "\t" + storage.get(key));
                    bw.newLine();
                }
                // Write server configuration to file
                bw.write(SnapshotManager.CONFIGURATION);
                bw.newLine();
                for (Integer key : servers.keySet()) {
                    bw.write(key + "\t" + servers.get(key));
                    bw.newLine();
                }
                // Write served clients to file
                bw.write(SnapshotManager.CLIENTS);
                bw.newLine();
                for (Integer key : servedClients.keySet()) {
                    bw.write(key + "\t" + servers.get(key));
                    bw.newLine();
                }
                bw.flush();
                try {
                    // Get lock and erase the log entries that were stored
                    server.getAppendLock();
                    manager.setLastStoredIndex(lastApplied);
                    manager.setLastSnapshot(new Snapshot(lastApplied, termOfLastApplied, storage, servers, servedClients));
                    server.cleanLogUntil(lastApplied);
                } finally {
                    server.releaseAppendLock();
                }
            } catch (IOException e) {
                logger.error("Exception when writing snapshot: ", e);
            } finally {
                try {
                    if (fos != null) {
                        fos.close();
                    }
                    if (bw != null) {
                        bw.close();
                    }
                } catch (IOException e) {
                    logger.warn("Exception when closing snapshot writing streams: ", e);
                }

            }

        }
    }
}
