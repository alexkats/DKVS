package com.katsman.dkvs.common.messages.ssp;

import com.katsman.dkvs.common.messages.DefaultMessage;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class AppendEntriesResult extends DefaultMessage {
    private final int term;
    private final boolean success;
    private final int length;
    private final int id;

    public AppendEntriesResult(SocketAddress address, int term, boolean success, int length, int id) {
        super(address);
        this.term = term;
        this.success = success;
        this.length = length;
        this.id = id;
    }

    public int getTerm() {
        return term;
    }

    public boolean isSuccess() {
        return success;
    }

    public int getLength() {
        return length;
    }

    public int getId() {
        return id;
    }

    public static AppendEntriesResult parse(SocketAddress address, Scanner scanner) {
        int term = scanner.nextInt();
        boolean success = scanner.nextBoolean();
        int length = scanner.nextInt();
        int id = scanner.nextInt();
        return new AppendEntriesResult(address, term, success, length, id);
    }

    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("AppendEntriesResult { term = ");
        sb.append(term);
        sb.append("; success = ");
        sb.append(success);
        sb.append("; length = ");
        sb.append(length);
        sb.append("; id = ");
        sb.append(id);
        sb.append(" }");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("AppendEntriesResult | ");
        sb.append(term);
        sb.append(" | ");
        sb.append(success);
        sb.append(" | ");
        sb.append(length);
        sb.append(" | ");
        sb.append(id);
        return sb.toString();
    }
}
