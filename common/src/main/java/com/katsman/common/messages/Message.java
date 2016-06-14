package com.katsman.common.messages;

import com.katsman.common.messages.csp.ClientServerRequest;

import java.net.SocketAddress;
import java.util.Scanner;

/**
 * @author Alexey Katsman
 * @since 12.06.16
 */

public interface Message {
    public static Message parse(SocketAddress address, Scanner scanner) {
        String s = scanner.next();

        switch (s) {
            case "client_server_request":
                return ClientServerRequest.parse(address, scanner);
            default:
                return null;
        }
    }

    public String print();
}
