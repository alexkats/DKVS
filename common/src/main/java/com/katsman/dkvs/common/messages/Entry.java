package com.katsman.dkvs.common.messages;

import com.katsman.dkvs.common.utils.Operation;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class Entry implements Message {
    private final int term;
    private final Operation operation;
    private final String key;
    private final String value;

    public Entry(int term, Operation operation, String key, String value) {
        this.term = term;
        this.operation = operation;
        this.key = key;
        this.value = value;
    }

    public int getTerm() {
        return term;
    }

    public Operation getOperation() {
        return operation;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public static Entry parse(Scanner scanner) {
        scanner.next();
        int term = scanner.nextInt();
        Operation operation = Operation.valueOf(scanner.next());
        String key = scanner.next();
        String value = scanner.nextLine().substring(1);
        return new Entry(term, operation, key, value);
    }

    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("Entry { term = ");
        sb.append(term);
        sb.append("; operation = ");
        sb.append(operation.name());
        sb.append("; key = ");
        sb.append(key == null ? "null" : key);
        sb.append("; value = ");
        sb.append(value == null ? "null" : value);
        sb.append(" }");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Entry ");
        sb.append(term);
        sb.append(" ");
        sb.append(operation.name());
        sb.append(" ");
        sb.append(key == null ? "null" : key);
        sb.append(" ");
        sb.append(value == null ? "null" : value);
        return sb.toString();
    }

    @Override
    public SocketAddress getAddress() {
        return null;
    }
}
