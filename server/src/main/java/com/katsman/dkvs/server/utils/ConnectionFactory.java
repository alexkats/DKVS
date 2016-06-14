package com.katsman.dkvs.server.utils;

import com.katsman.dkvs.common.messages.Message;
import com.katsman.dkvs.server.workers.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Alexey Katsman
 * @since 15.06.16
 */

public class ConnectionFactory {
    private final Map<SocketAddress, Connection> connections;
    private final Server server;
    private final ServerSocket serverSocket;
    private final int timeout;
    private final ConnectionReader reader;
    private final ConnectionWriter writer;

    private volatile boolean active;

    public ConnectionFactory(Configuration configuration, Server server) throws IOException {
        connections = new HashMap<>();
        this.server = server;
        serverSocket = new ServerSocket();
        String host = configuration.getHosts().get(configuration.getNodeNumber());
        int port = configuration.getPorts().get(configuration.getNodeNumber());
        serverSocket.bind(new InetSocketAddress(host, port));
        timeout = configuration.getTimeout();
        reader = new ConnectionReader();
        writer = new ConnectionWriter();
        active = true;
    }

    private class ConnectionReader extends Thread {
        @Override
        public void run() {
            while (active) {
                try {
                    Socket socket = serverSocket.accept();
                    SocketAddress address = socket.getRemoteSocketAddress();

                    synchronized (connections) {
                        if (connections.containsKey(address)) {
                            connections.get(address).close();
                        }

                        Connection connection = new Connection(socket, server);
                        connections.put(address, connection);
                        connection.start();
                    }
                } catch (IOException ignored) {

                }
            }
        }
    }

    private class ConnectionWriter extends Thread {
        @Override
        public void run() {
            while (active) {
                Message message;

                try {
                    message = server.get();
                } catch (InterruptedException e) {
                    close();
                    return;
                }

                synchronized (connections) {
                    SocketAddress address = message.getAddress();

                    if (!connections.containsKey(address) || !connections.get(address).isActive()) {
                        Socket socket = new Socket();

                        try {
                            socket.connect(address, timeout);
                            Connection connection = new Connection(socket, server);
                            connections.put(address, connection);
                            connection.start();
                        } catch (IOException ignored) {

                        }
                    }

                    if (connections.get(address) != null) {
                        connections.get(address).send(message);
                    }
                }
            }
        }
    }

    public void start() {
        reader.start();
        writer.start();
    }

    public void close() {
        active = false;

        try {
            serverSocket.close();
        } catch (IOException e) {
            synchronized (System.out) {
                System.out.println("Can't close server socket: " + e.getMessage());
            }
        }

        writer.interrupt();

        synchronized (connections) {
            connections.values().forEach(com.katsman.dkvs.server.utils.Connection::close);
        }
    }
}
