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

public class ClientServerRequest extends DefaultMessage {
    private final String key;
    private final String value;
    private final Operation operation;
    private final List<InetSocketAddress> redirections;

    public ClientServerRequest(SocketAddress address, String key, String value, Operation operation, List<InetSocketAddress> redirections) {
        super(address);
        this.key = key;
        this.value = value;
        this.operation = operation;
        this.redirections = redirections;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public Operation getOperation() {
        return operation;
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

    public static ClientServerRequest parse(SocketAddress address, Scanner scanner) {
        Operation operation = Operation.valueOf(scanner.next());
        String key = scanner.next();
        String value = scanner.nextLine().substring(1);
        List<InetSocketAddress> redirections = receiveRedirections(scanner);
        return new ClientServerRequest(address, key, value, operation, redirections);
    }

    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("ClientServerRequest { operation = ");
        sb.append(operation.name());
        sb.append("; key = ");
        sb.append(key == null ? "null" : key);
        sb.append("; value = ");
        sb.append(value == null ? "null" : value);
        sb.append("; redirections = [");
        sb.append(redirections.stream().map(InetSocketAddress::toString).collect(Collectors.joining(", ")));
        sb.append("] }");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ClientServerRequest ");
        sb.append(operation.name());
        sb.append(" ");
        sb.append(key == null ? "null" : key);
        sb.append(" ");
        sb.append(value == null ? "null" : value);
        sb.append("\n");
        sb.append(redirections.size());
        sb.append("\n");
        sb.append(redirections.stream().map(e -> (e.getHostName() + " " + e.getPort())).collect(Collectors.joining("\n")));
        return sb.toString();
    }
}
