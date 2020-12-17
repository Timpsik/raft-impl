package com.raft.client;

import com.raft.requests.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Random;

public class LatencyClient {

    /**
     * Number of expected command line arguments
     */
    public static final int EXPECTED_ARGUMENTS = 4;

    /**
     * Number of clients, which are created.
     */
    private static final int CLIENT_ID = 1;

    /**
     * Zookeeper cluster address, "serverIp1:port,serverIp2:port,...."
     */
    private static final int CLUSTER_ADDRESS_IDX = 0;

    /**
     * Start time of the benchmark test. Should be in future to ensure all clients are ready.
     */
    private static final int START_TIME_IDX = 2;


    private final Logger logger = LogManager.getLogger(LatencyClient.class);

    private String[] clusterAddresses;
    private String leaderAddress = "";
    private Random random = new Random();
    private final int clientId;
    private long requestNr = 0;
    private long benchmarkStartTime;

    private final long N = 1000;

    public LatencyClient(String clusterAddress, int clientId, String startTime) {
        parseAndCheckStartTime(startTime);
        this.clusterAddresses = clusterAddress.split(",");
        this.clientId = clientId;
    }

    public static void main(String[] args) throws IOException {

        LatencyClient raftClient = new LatencyClient(args[CLUSTER_ADDRESS_IDX], Integer.parseInt(args[CLIENT_ID]), args[START_TIME_IDX]);
        raftClient.start();
    }


    /**
     * Check if the start time is long and start is in the future,
     * otherwise it isn't possible to calculate correct throughput.
     *
     * @param startTimeString Benchmark start time in String
     */
    private void parseAndCheckStartTime(String startTimeString) {
        try {
            benchmarkStartTime = Long.parseLong(startTimeString);
            if (System.currentTimeMillis() > benchmarkStartTime) {
                throw new IllegalArgumentException("Benchmark start time is in the past " +
                        "Current time: " + System.currentTimeMillis() + ", Start: " + benchmarkStartTime);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Argument at index " + START_TIME_IDX +
                    " is expected to be long. Given: " + startTimeString +
                    " . It is the start time of benchmark.");
        }
    }

    private void start() {
        logger.warn("Creating client");
        Socket socket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            int i = 0;
            boolean started = false;
            while (i < N) {
                if (!started && System.currentTimeMillis() > benchmarkStartTime) {
                    started = true;
                }
                String serverToConnect = leaderAddress;
                if ("".equals(serverToConnect)) {
                    serverToConnect = clusterAddresses[random.nextInt(clusterAddresses.length)];
                }

                logger.info("Connecting to " + serverToConnect);
                RaftRequest r = new ChangeStateRequest("" + clientId, 2,clientId, requestNr);
                socket = new Socket(serverToConnect.split(":")[0], Integer.parseInt(serverToConnect.split(":")[1]));
                out = new ObjectOutputStream(socket.getOutputStream());
                requestNr++;
                out.writeObject(r);
                in = new ObjectInputStream(socket.getInputStream());
                RaftResponse response = (RaftResponse) in.readObject();
                while(!response.isSuccess() && response.getCause() == ErrorCause.NOT_LEADER) {
                    logger.info("Did not send the request to leader, re-send");
                    try {
                        out.close();
                        in.close();
                        socket.close();
                    } catch (IOException e) {
                        logger.error("Could not close old socket: ", e);
                    }
                    serverToConnect = response.getLeaderAddress();
                    leaderAddress = serverToConnect;
                    socket = new Socket(serverToConnect.split(":")[0], Integer.parseInt(serverToConnect.split(":")[1]));
                    out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(r);
                    in = new ObjectInputStream(socket.getInputStream());
                    response = (RaftResponse) in.readObject();
                }
                if (!response.isSuccess()) {
                    logger.warn("Request failed: " + response.getCause().name());

                }
                logger.info("Request was successful");
                i++;
            }
            logger.warn(System.currentTimeMillis());
        } catch (IOException | ClassNotFoundException e) {
            logger.info("Error: ", e);
        } finally {
            if (socket != null) {
                try {
                    if (out != null) {
                        out.close();
                    }
                    if (in != null) {
                        in.close();
                    }
                    socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
