package com.katsman.dkvs.common.messages.ssp;

import com.katsman.dkvs.common.messages.DefaultMessage;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class VoteRequest extends DefaultMessage {
    private final int term;
    private final int candidateId;
    private final int lastLogIndex;
    private final int lastLogTerm;

    public VoteRequest(SocketAddress address, int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        super(address);
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public static VoteRequest parse(SocketAddress address, Scanner scanner) {
        int term = scanner.nextInt();
        int candidateId = scanner.nextInt();
        int lastLogIndex = scanner.nextInt();
        int lastLogTerm = scanner.nextInt();
        return new VoteRequest(address, term, candidateId, lastLogIndex, lastLogTerm);
    }

    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("VoteRequest { term = ");
        sb.append(term);
        sb.append("; candidateId = ");
        sb.append(candidateId);
        sb.append("; lastLogIndex = ");
        sb.append(lastLogIndex);
        sb.append("; lastLogTerm = ");
        sb.append(lastLogTerm);
        sb.append(" }");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VoteRequest | ");
        sb.append(term);
        sb.append(" | ");
        sb.append(candidateId);
        sb.append(" | ");
        sb.append(lastLogIndex);
        sb.append(" | ");
        sb.append(lastLogTerm);
        return sb.toString();
    }
}
