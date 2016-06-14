package com.katsman.common.messages.csp;

import com.katsman.common.messages.DefaultMessage;
import com.katsman.common.utils.Operation;

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
    private final boolean ok;
    private final List<InetSocketAddress> redirections;

    public ClientServerResponse(SocketAddress address, Operation operation, String result, boolean ok, List<InetSocketAddress> redirections) {
        super(address);
        this.operation = operation;
        this.result = result;
        this.ok = ok;
        this.redirections = redirections;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getResult() {
        return result;
    }

    public boolean isOk() {
        return ok;
    }

    public List<InetSocketAddress> getRedirections() {
        return redirections;
    }

    private static List<InetSocketAddress> receiveRedirections(Scanner scanner) {
        List<InetSocketAddress> redirections = new ArrayList<>();
        int n = scanner.nextInt();

        for (int i = 0; i < n; i++) {
            redirections.add(new InetSocketAddress(scanner.next(), scanner.nextInt()));
        }

        return redirections;
    }

    public static ClientServerResponse parse(SocketAddress address, Scanner scanner) {
        Operation operation = Operation.valueOf(scanner.next());
        boolean ok = scanner.nextBoolean();
        String result = scanner.nextLine().substring(1);
        List<InetSocketAddress> redirections = receiveRedirections(scanner);
        return new ClientServerResponse(address, operation, result, ok, redirections);
    }

    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("ClientServerResponse { operation = ");
        sb.append(operation.name());
        sb.append("; ok = ");
        sb.append(ok);
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
        sb.append("ClientServerResponse | ");
        sb.append(operation.name());
        sb.append(" | ");
        sb.append(ok);
        sb.append(" | ");
        sb.append(result == null ? "null" : result);
        sb.append("\n");
        sb.append(redirections.size());
        sb.append("\n");
        sb.append(redirections.stream().map(e -> (e.getHostName() + " | " + e.getPort())).collect(Collectors.joining("\n")));
        return sb.toString();
    }
}
