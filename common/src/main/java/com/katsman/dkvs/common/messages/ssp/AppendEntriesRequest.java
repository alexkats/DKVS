package com.katsman.dkvs.common.messages.ssp;

import com.katsman.dkvs.common.messages.DefaultMessage;
import com.katsman.dkvs.common.messages.Message;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class AppendEntriesRequest extends DefaultMessage {
    private final int term;
    private final int leaderId;
    private final int prevLogIndex;
    private final int prevLogTerm;
    private final List<Message> entries;
    private final int leaderCommit;

    public AppendEntriesRequest(SocketAddress address, int term, int leaderId, int prevLogIndex, int prevLogTerm, List<Message> entries, int leaderCommit) {
        super(address);
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public int getTerm() {
        return term;
    }

    public int getLeaderId() {
        return leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public List<Message> getEntries() {
        return entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    private static List<Message> receiveEntries(Scanner scanner) {
        List<Message> entries = new ArrayList<>();
        int n = scanner.nextInt();

        for (int i = 0; i < n; i++) {
            entries.add(Message.parse(scanner));
        }

        return entries;
    }

    public static AppendEntriesRequest parse(SocketAddress address, Scanner scanner) {
        int term = scanner.nextInt();
        int leaderId = scanner.nextInt();
        int prevLogIndex = scanner.nextInt();
        int prevLogTerm = scanner.nextInt();
        List<Message> entries = receiveEntries(scanner);
        int leaderCommit = scanner.nextInt();
        return new AppendEntriesRequest(address, term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("AppendEntriesRequest { term = ");
        sb.append(term);
        sb.append("; leaderId = ");
        sb.append(leaderId);
        sb.append("; prevLogIndex = ");
        sb.append(prevLogIndex);
        sb.append("; prevLogTerm = ");
        sb.append(prevLogTerm);
        sb.append("; entries = [");
        sb.append(entries.stream().map(Object::toString).collect(Collectors.joining(", ")));
        sb.append("]; leaderCommit = ");
        sb.append(leaderCommit);
        sb.append(" }");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("AppendEntriesRequest ");
        sb.append(term);
        sb.append(" ");
        sb.append(leaderId);
        sb.append(" ");
        sb.append(prevLogIndex);
        sb.append(" ");
        sb.append(prevLogTerm);
        sb.append(" ");
        sb.append(entries.size());
        sb.append("\n");
        sb.append(entries.stream().map(Object::toString).collect(Collectors.joining("\n")));
        sb.append("\n");
        sb.append(leaderCommit);
        return sb.toString();
    }
}
