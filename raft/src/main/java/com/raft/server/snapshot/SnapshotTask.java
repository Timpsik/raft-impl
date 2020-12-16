package com.raft.server.snapshot;

import com.raft.server.RaftServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class SnapshotTask implements Runnable {
    private static Logger logger = LogManager.getLogger(SnapshotTask.class);

    private final SnapshotManager manager;

    public SnapshotTask(SnapshotManager manager) {
        this.manager = manager;
    }


    @Override
    public void run() {
        logger.info("Running snapshot");
        if (manager.isStateChanged()) {
            RaftServer server = manager.getServer();
            int lastApplied = -1;
            long termOfLastApplied = -1;
            Map<String, Integer> state = new HashMap<>();
            Map<Integer, String> servers = new HashMap<>();

            try {
                server.getAppendLock();
                lastApplied = server.getLastApplied().get();
                termOfLastApplied = server.getTermOfLastApplied();
                state.putAll(server.getCurrentMachineState());
                servers.putAll(server.getServers());
            } finally {
                server.releaseAppendLock();
            }
            if (lastApplied >= 0 && termOfLastApplied >= 0) {
                FileOutputStream fos = null;
                BufferedWriter bw = null;
                try {
                    File file = new File(manager.getFileName());
                    fos = new FileOutputStream(file);

                    bw = new BufferedWriter(new OutputStreamWriter(fos, StandardCharsets.UTF_8.name()));
                    bw.write(Long.toString(termOfLastApplied));
                    bw.newLine();
                    bw.write(Integer.toString(lastApplied));
                    bw.newLine();
                    for (String key : state.keySet()) {
                        bw.write(key + "\t" + state.get(key));
                        bw.newLine();
                    }
                    bw.write(SnapshotManager.CONFIGURATION);
                    bw.newLine();
                    for (Integer key : servers.keySet()) {
                        bw.write(key + "\t" + servers.get(key));
                        bw.newLine();
                    }
                    bw.flush();
                    try {
                        server.getAppendLock();
                        manager.setLastStoredIndex(lastApplied);
                        manager.setLastSnapshot(new Snapshot(lastApplied, termOfLastApplied, state, servers));
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
}
