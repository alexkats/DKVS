package com.katsman.dkvs.common.messages.csp;

import com.katsman.dkvs.common.messages.DefaultMessage;
import com.katsman.dkvs.common.utils.Operation;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class ClientServerResponse extends DefaultMessage {
    private final Operation operation;
    private final String result;
    private final boolean success;
    private final List<InetSocketAddress> redirections;

    public ClientServerResponse(SocketAddress address, Operation operation, String result, boolean success, List<InetSocketAddress> redirections) {
        super(address);
        this.operation = operation;
        this.result = result;
        this.success = success;
        this.redirections = redirections;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getResult() {
        return result;
    }

    public boolean isSuccess() {
        return success;
    }

    public List<InetSocketAddress> getRedirections() {
        return redirections;
    }

    private static List<InetSocketAddress> receiveRedirections(Scanner scanner) {
        List<InetSocketAddress> redirections = new ArrayList<>();

        for (int i = 0; i < scanner.nextInt(); i++) {
            redirections.add(new InetSocketAddress(scanner.next(), scanner.nextInt()));
        }

        return redirections;
    }

    public static ClientServerResponse parse(SocketAddress address, Scanner scanner) {
        Operation operation = Operation.valueOf(scanner.next());
        boolean success = scanner.nextBoolean();
        String result = scanner.nextLine().substring(1);
        List<InetSocketAddress> redirections = receiveRedirections(scanner);
        return new ClientServerResponse(address, operation, result, success, redirections);
    }

    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("ClientServerResponse { operation = ");
        sb.append(operation.name());
        sb.append("; success = ");
        sb.append(success);
        sb.append("; result = ");
        sb.append(result == null ? "null" : result);
        sb.append("; redirections = [");
        sb.append(redirections.stream().map(InetSocketAddress::toString).collect(Collectors.joining(", ")));
        sb.append("] }");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ClientServerResponse ");
        sb.append(operation.name());
        sb.append(" ");
        sb.append(success);
        sb.append(" ");
        sb.append(result == null ? "null" : result);
        sb.append("\n");
        sb.append(redirections.size());
        sb.append("\n");
        sb.append(redirections.stream().map(e -> (e.getHostName() + " " + e.getPort())).collect(Collectors.joining("\n")));
        return sb.toString();
    }
}
