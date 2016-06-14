package com.katsman.common.messages;

import com.katsman.common.messages.csp.ClientServerRequest;
import com.katsman.common.messages.csp.ClientServerResponse;
import com.katsman.common.messages.ssp.AppendEntriesRequest;
import com.katsman.common.messages.ssp.AppendEntriesResult;
import com.katsman.common.messages.ssp.VoteRequest;
import com.katsman.common.messages.ssp.VoteResponse;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * @author Alexey Katsman
 * @since 12.06.16
 */

public interface Message {
    public static Message parse(Scanner scanner) {
        return Entry.parse(scanner);
    }

    public static Message parse(SocketAddress address, Scanner scanner) {
        String s = scanner.next();

        switch (s) {
            case "ClientServerRequest":
                return ClientServerRequest.parse(address, scanner);
            case "ClientServerResponse":
                return ClientServerResponse.parse(address, scanner);
            case "VoteRequest":
                return VoteRequest.parse(address, scanner);
            case "VoteResponse":
                return VoteResponse.parse(address, scanner);
            case "AppendEntriesRequest":
                return AppendEntriesRequest.parse(address, scanner);
            case "AppendEntriesResult":
                return AppendEntriesResult.parse(address, scanner);
            default:
                return null;
        }
    }

    public String print();
}
