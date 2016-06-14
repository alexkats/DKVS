package com.katsman.dkvs.common.messages;

import java.net.SocketAddress;

/**
 * @author Alexey Katsman
 * @since 12.06.16
 */

public abstract class DefaultMessage implements Message {
    private final SocketAddress address;

    protected DefaultMessage(SocketAddress address) {
        this.address = address;
    }

    @Override
    public SocketAddress getAddress() {
        return address;
    }
}
