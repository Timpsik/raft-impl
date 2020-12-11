package com.raft.client;

import java.io.*;
import java.net.Socket;
import java.util.Random;

import com.raft.requests.ChangeStateRequest;
import com.raft.requests.ChangeStateResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RaftClient {

    private final Logger logger = LogManager.getLogger(RaftClient.class);

    private String[] clusterAddresses;
    private int leaderIndex;
    private Random random = new Random();

    public RaftClient(String clusterAddress) {
        this.clusterAddresses = clusterAddress.split(",");
    }

    public static void main(String[] args) throws IOException {
        RaftClient raftClient = new RaftClient(args[0]);
        raftClient.start();
    }

    private void start() throws IOException {
        logger.info("Creating client");
        Socket socket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {

            while (true) {
                int serverToConnect = leaderIndex;
                if (serverToConnect != -1) {
                    serverToConnect = random.nextInt(clusterAddresses.length);
                }
                logger.info("Connecting to " + clusterAddresses[serverToConnect]);
                //Enter data using BufferReader
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                System.out.println("Enter variable name or empty String to exit: ");
                String var = reader.readLine().strip();
                if ("".equals(var)) {
                    break;
                }
                System.out.println("Enter vale: ");
                int value = Integer.parseInt(reader.readLine());
                socket = new Socket(clusterAddresses[serverToConnect].split(":")[0], Integer.parseInt(clusterAddresses[serverToConnect].split(":")[1]));
                out = new ObjectOutputStream(socket.getOutputStream());
                ChangeStateRequest r = new ChangeStateRequest(var, value);
                out.writeObject(r);
                in = new ObjectInputStream(socket.getInputStream());
                ChangeStateResponse response = (ChangeStateResponse) in.readObject();
                while(!response.isSuccess()) {
                    logger.info("Did not send the request to leader, re-send");
                    try {
                        out.close();
                        in.close();
                        socket.close();
                    } catch (IOException e) {
                        logger.error("Could not close old socket: ", e);
                        e.printStackTrace();
                    }
                    leaderIndex = response.getLeaderId();
                    socket = new Socket(clusterAddresses[leaderIndex].split(":")[0], Integer.parseInt(clusterAddresses[leaderIndex].split(":")[1]));
                    out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(r);
                    in = new ObjectInputStream(socket.getInputStream());
                    response = (ChangeStateResponse) in.readObject();
                }
                logger.info("Request was successful");
            }


        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
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
