package com.raft.server.connection;

import com.raft.server.rpc.ServerRequest;
import com.raft.server.rpc.ServerResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Output connection to other server
 */
public class ServerOutConnection {

    private static Logger logger = LogManager.getLogger(ServerOutConnection.class);
    private final String serverAddress;
    private Socket socket;
    private ObjectInputStream in;
    private ObjectOutputStream out;

    public ServerOutConnection(String serverAddress) {
        this.serverAddress = serverAddress;
        try {
            initialize();
        } catch (IOException e) {
            logger.warn("Connection error when connecting to: " + serverAddress, e);
            closeConnections();
        }
    }

    /**
     * Try to make the connection to other server
     * @throws IOException
     */
    private void initialize() throws IOException {
        socket = new Socket(serverAddress.split(":")[0], Integer.parseInt(serverAddress.split(":")[1]));
        socket.setKeepAlive(true);
        out = new ObjectOutputStream(socket.getOutputStream());
        in = new ObjectInputStream(socket.getInputStream());
    }

    /**
     * Close the connection to other server
     */
    private void closeConnections() {
        try {
            if (socket != null) {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
                socket.close();
            }
        } catch (IOException e) {
            logger.warn("Connection error when closing socket to: " + serverAddress, e);
        } finally {
            socket = null;
        }
    }

    /**
     * Send request to other server
     * @param request
     * @return
     */
    public synchronized ServerResponse sendRequestToServer(ServerRequest request){
        try {
            if (socket == null) {
                logger.info("Connection not initialized, creating to: " + serverAddress);
                initialize();
            }
            out.reset();
            out.writeObject(request);
            return (ServerResponse) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            logger.info("Connection error when connecting to: " + serverAddress, e);
            closeConnections();
        }
        return null;
    }
}
