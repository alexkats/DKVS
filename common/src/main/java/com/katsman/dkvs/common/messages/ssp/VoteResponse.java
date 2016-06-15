package com.katsman.dkvs.common.messages.ssp;

import com.katsman.dkvs.common.messages.DefaultMessage;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class VoteResponse extends DefaultMessage {
    private final int term;
    private final boolean voteGranted;

    public VoteResponse(SocketAddress address, int term, boolean voteGranted) {
        super(address);
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public static VoteResponse parse(SocketAddress address, Scanner scanner) {
        int term = scanner.nextInt();
        boolean voteGranted = scanner.nextBoolean();
        return new VoteResponse(address, term, voteGranted);
    }

    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("VoteResponse { term = ");
        sb.append(term);
        sb.append("; voteGranted = ");
        sb.append(voteGranted);
        sb.append(" }");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VoteResponse ");
        sb.append(term);
        sb.append(" ");
        sb.append(voteGranted);
        return sb.toString();
    }
}
