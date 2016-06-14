package com.katsman.common.messages;

import java.net.SocketAddress;

/**
 * @author Alexey Katsman
 * @since 12.06.16
 */

public abstract class DefaultMessage implements Message {
    public final SocketAddress address;

    public DefaultMessage(SocketAddress address) {
        this.address = address;
    }
}
