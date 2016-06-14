package com.katsman.dkvs.server.utils;

import com.katsman.dkvs.common.messages.Message;
import com.katsman.dkvs.server.workers.Server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Scanner;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class Connection {
    private final Socket socket;
    private final QueryReader reader;
    private final QueryWriter writer;

    private volatile boolean active;

    public Connection(Socket socket, Server server) throws IOException {
        this.socket = socket;
        reader = new QueryReader(socket, server);
        writer = new QueryWriter(socket);
        active = true;
    }

    public boolean isActive() {
        return active;
    }

    private class QueryReader extends Thread {
        private final Scanner scanner;
        private final Server server;

        public QueryReader(Socket socket, Server server) throws IOException {
            scanner = new Scanner(socket.getInputStream());
            this.server = server;
        }

        @Override
        public void run() {
            while (active) {
                try {
                    if (scanner.next().equals("message")) {
                        Message message = Message.parse(socket.getRemoteSocketAddress(), scanner);

                        if (message != null) {
                            server.add(message);
                        } else {
                            close();
                        }
                    } else {
                        close();
                    }
                } catch (NoSuchElementException | IllegalStateException e) {
                    close();
                }
            }
        }
    }

    private class QueryWriter extends Thread {
        private final PrintWriter writer;
        private final Queue<Message> requests;

        public QueryWriter(Socket socket) throws IOException {
            writer = new PrintWriter(socket.getOutputStream());
            requests = new ArrayDeque<>();
        }

        @Override
        public void run() {
            while (active) {
                Message message = null;

                while (message == null) {
                    synchronized (requests) {
                        if (!requests.isEmpty()) {
                            message = requests.poll();
                        } else {
                            try {
                                requests.wait();
                            } catch (InterruptedException e) {
                                return;
                            }
                        }
                    }
                }

                writer.write("message " + message + "\n");
                writer.flush();
            }
        }

        public void send(Message message) {
            synchronized (requests) {
                requests.add(message);
                requests.notify();
            }
        }
    }

    public void send(Message message) {
        writer.send(message);
    }

    public void start() {
        reader.start();
        writer.start();
    }

    public void close() {
        active = false;

        try {
            socket.close();
        } catch (IOException ignored) {

        } finally {
            writer.interrupt();
        }
    }
}
