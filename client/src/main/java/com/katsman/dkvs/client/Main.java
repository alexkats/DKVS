package com.katsman.dkvs.client;

import com.katsman.dkvs.common.messages.Message;
import com.katsman.dkvs.common.messages.csp.ClientServerRequest;
import com.katsman.dkvs.common.messages.csp.ClientServerResponse;
import com.katsman.dkvs.common.utils.Operation;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class Main {
    private static Scanner in = null;
    private static PrintWriter out = null;
    private static Scanner scanner = new Scanner(System.in);
    private static boolean connected = false;

    public static void main(String[] args) {
        printHelp();

        while (true) {
            System.out.println("> ");
            String command = scanner.next().toLowerCase();

            switch (command) {
                case "connect":
                    connect();
                    break;
                case "disconnect":
                    disconnect();
                    break;
                case "set":
                    set();
                    break;
                case "get":
                    get();
                    break;
                case "delete":
                    delete();
                    break;
                case "ping":
                    ping();
                    break;
                case "exit":
                    exit();
                    return;
                case "help":
                    printHelp();
                    break;
                default:
                    printUnknown();
            }
        }
    }

    private static void connect() {
        try {
            if (connected) {
                System.out.println("Already connected!");
            } else {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(scanner.next(), scanner.nextInt()));
                in = new Scanner(socket.getInputStream());
                out = new PrintWriter(socket.getOutputStream());
                connected = true;
            }
        } catch (IOException e) {
            System.out.println("Can't connect: " + e.getMessage());
            System.out.flush();
        }
    }

    private static void disconnect() {
        if (connected) {
            System.out.println("Already connected!");
        } else {
            out.write("end");
            out.close();
            in.close();
            connected = false;
        }
    }

    private static void set() {
        if (!connected) {
            printNotConnected();
        } else {
            sendRequest(out, new ClientServerRequest(null, scanner.next(), scanner.nextLine().substring(1), Operation.SET, new ArrayList<>()));
            Message message = receiveResponse(in);

            if (message != null && message instanceof ClientServerResponse) {
                ClientServerResponse response = (ClientServerResponse) message;

                if (response.isSuccess()) {
                    System.out.println("Stored successfully");
                } else {
                    System.out.println("Failed: " + response.getResult());
                }
            } else {
                printError();
            }
        }
    }

    private static void get() {
        if (!connected) {
            printNotConnected();
        } else {
            sendRequest(out, new ClientServerRequest(null, scanner.next(), null, Operation.GET, new ArrayList<>()));
            Message message = receiveResponse(in);

            if (message != null && message instanceof ClientServerResponse) {
                ClientServerResponse response = (ClientServerResponse) message;

                if (response.isSuccess()) {
                    System.out.println(response.getResult());
                } else {
                    System.out.println("Failed: " + (response.getResult().equals("null") ? "key doesn't exist" : response.getResult()));
                }
            } else {
                printError();
            }
        }
    }

    private static void delete() {
        if (!connected) {
            printNotConnected();
        } else {
            sendRequest(out, new ClientServerRequest(null, scanner.next(), null, Operation.DELETE, new ArrayList<>()));
            Message message = receiveResponse(in);

            if (message != null && message instanceof ClientServerResponse) {
                ClientServerResponse response = (ClientServerResponse) message;

                if (response.isSuccess()) {
                    System.out.println("Deleted");
                } else {
                    System.out.println("Failed: " + (response.getResult().equals("null") ? "key doesn't exist" : response.getResult()));
                }
            } else {
                printError();
            }
        }
    }

    private static void ping() {
        if (!connected) {
            printNotConnected();
        } else {
            sendRequest(out, new ClientServerRequest(null, null, null, Operation.PING, new ArrayList<>()));
            Message message = receiveResponse(in);

            if (message != null && message instanceof ClientServerResponse) {
                ClientServerResponse response = (ClientServerResponse) message;

                if (response.isSuccess()) {
                    System.out.println("Pong");
                } else {
                    printError();
                }
            } else {
                printError();
            }
        }
    }

    private static void exit() {
        if (connected) {
            out.write("end");
            out.close();
            in.close();
            connected = false;
        }
    }

    private static void printHelp() {
        System.out.println("Type \"connect <host address> <port number>\" to connect (e.g. \"connect 127.0.0.1 1234\")");
        System.out.println("Type \"disconnect\" to disconnect from server");
        System.out.println("Type \"set <key> <value>\" to set value for key (e.g. \"set Hey Hello\")");
        System.out.println("Type \"get <key>\" to get value for key (e.g. \"get Hey\")");
        System.out.println("Type \"delete <key>\" to delete value for key (e.g. \"delete Hey\")");
        System.out.println("Type \"ping\" to ping server");
        System.out.println("Type \"exit\" to exit");
        System.out.println("Type \"help\" to get this help");
    }

    private static void printNotConnected() {
        System.out.println("Not connected");
    }

    private static void printUnknown() {
        System.out.println("Unrecognized command!");
        System.out.println("Please, try again");
        System.out.println("To get help type \"help\"");
    }

    private static void sendRequest(PrintWriter writer, Message message) {
        writer.write("message " + message + "\n");
        writer.flush();
    }

    private static Message receiveResponse(Scanner scanner) {
        if (scanner.next().equals("message")) {
            return Message.parse(scanner);
        } else {
            return null;
        }
    }

    private static void printError() {
        System.out.println("Unknown error happened!");
    }
}
