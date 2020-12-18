package com.raft.client;

import com.raft.requests.ChangeStateRequest;
import com.raft.requests.ErrorCause;
import com.raft.requests.RaftRequest;
import com.raft.requests.RaftResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Random;

public class LatencyClient {
    private static final Logger logger = LogManager.getLogger(LatencyClient.class);

    /**
     * Number of expected command line arguments
     */
    public static final int EXPECTED_ARGUMENTS = 2;

    /**
     * Zookeeper cluster address, "serverIp1:port,serverIp2:port,...."
     */
    private static final int CLUSTER_ADDRESS_IDX = 0;

    /**
     * Number of clients, which are created.
     */
    private static final int CLIENT_ID = 1;


    /**
     * String containing all the addresses of servers separated by commas.
     */
    private String[] clusterAddresses;

    /**
     * Address of the leader server
     */
    private String leaderAddress = "";

    private Random random = new Random();

    /**
     * Id of the client, each client should have their own id.
     */
    private final int clientId;

    /**
     * Request number of the client, increment it after every request.
     */
    private long requestNr = 0;

    private final long N = 1000;

    public LatencyClient(String clusterAddress, int clientId) {
        this.clusterAddresses = clusterAddress.split(",");
        this.clientId = clientId;
    }

    public static void main(String[] args) throws IOException {
        if (args.length != EXPECTED_ARGUMENTS) {
            logger.error("Wrong number of arguments given. Expected 2, but got " + args.length);
            return;
        }
        LatencyClient raftClient = new LatencyClient(args[CLUSTER_ADDRESS_IDX], Integer.parseInt(args[CLIENT_ID]));
        raftClient.start();
    }

    private void start() {
        logger.warn("Creating client");
        Socket socket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {
            int i = 0;
            while (i < N) {
                String serverToConnect = leaderAddress;
                // Select random server to connect
                if ("".equals(serverToConnect)) {
                    serverToConnect = clusterAddresses[random.nextInt(clusterAddresses.length)];
                }

                logger.info("Connecting to " + serverToConnect);
                // Make the variable storing request
                RaftRequest r = new ChangeStateRequest("" + clientId, 2, clientId, requestNr);
                socket = new Socket(serverToConnect.split(":")[0], Integer.parseInt(serverToConnect.split(":")[1]));
                out = new ObjectOutputStream(socket.getOutputStream());
                requestNr++;
                out.writeObject(r);
                in = new ObjectInputStream(socket.getInputStream());
                RaftResponse response = (RaftResponse) in.readObject();
                // Resend the request if it wasn't sent to leader
                while (!response.isSuccess() && response.getCause() == ErrorCause.NOT_LEADER) {
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
            // print the end time of finishing requests
            logger.warn(System.currentTimeMillis());
        } catch (IOException | ClassNotFoundException e) {
            logger.warn("Error: ", e);
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
                    logger.warn("Exception when closing socket: ", e);
                }
            }
        }
    }
}
