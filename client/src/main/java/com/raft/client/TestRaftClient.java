package com.raft.client;

import java.io.*;
import java.net.Socket;
import java.util.Random;

import com.raft.requests.AddServerRequest;
import com.raft.requests.ChangeStateRequest;
import com.raft.requests.RaftRequest;
import com.raft.requests.RaftResponse;
import com.raft.requests.ReadRequest;
import com.raft.requests.ReadResponse;
import com.raft.requests.RemoveServerRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestRaftClient {

    private final Logger logger = LogManager.getLogger(TestRaftClient.class);

    private String[] clusterAddresses;
    private String leaderAddress = "";
    private Random random = new Random();
    private final int clientId;
    private long requestNr = 0;

    public TestRaftClient(String clusterAddress, int clientId) {
        this.clusterAddresses = clusterAddress.split(",");
        this.clientId = clientId;
    }

    public static void main(String[] args) throws IOException {
        TestRaftClient raftClient = new TestRaftClient(args[0], Integer.parseInt(args[1]));
        raftClient.start();
    }

    private void start() throws IOException {
        logger.info("Creating client");
        Socket socket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;
        try {

            while (true) {
                String serverToConnect = leaderAddress;
                if ("".equals(serverToConnect)) {

                    serverToConnect = clusterAddresses[random.nextInt(clusterAddresses.length)];
                }
                logger.info("Connecting to " + serverToConnect);
                //Enter data using BufferReader
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                System.out.println("Enter variable name or empty String to exit: ");
                String var = reader.readLine().strip();
                if ("".equals(var)) {
                    break;
                }
                RaftRequest r;
                if ("read".equals(var)) {
                    System.out.println("Enter variable name: ");
                    String readVar = reader.readLine().strip();
                    r = new ReadRequest(readVar,clientId,requestNr);
                } else if("add".equals(var)) {
                    System.out.println("Enter new Server id ");
                    int value = Integer.parseInt(reader.readLine());
                    System.out.println("Enter server address");
                    String  address = reader.readLine();
                    r = new AddServerRequest(address, value, clientId, requestNr);
                } else if("remove".equals(var)) {
                    System.out.println("Enter removed Server id ");
                    int value = Integer.parseInt(reader.readLine());
                    r = new RemoveServerRequest(value, clientId, requestNr);
                } else {
                    System.out.println("Enter value: ");
                    int value = Integer.parseInt(reader.readLine());
                    r = new ChangeStateRequest(var, value, clientId, requestNr);
                }
                socket = new Socket(serverToConnect.split(":")[0], Integer.parseInt(serverToConnect.split(":")[1]));
                out = new ObjectOutputStream(socket.getOutputStream());
                requestNr++;
                out.writeObject(r);
                in = new ObjectInputStream(socket.getInputStream());
                RaftResponse response = (RaftResponse) in.readObject();
                while(!response.isSuccess()) {
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
                if(response instanceof ReadResponse) {
                    System.out.println("Value stored: "  + ((ReadResponse) response).getValue());
                }
                logger.info("Request was successful");
            }
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
