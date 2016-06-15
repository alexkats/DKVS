package com.katsman.dkvs.server.utils;

import com.katsman.dkvs.common.utils.ParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class Configuration {
    private final int serversCount;
    private final int nodeNumber;
    private final int timeout;
    private final List<String> hosts;
    private final List<Integer> ports;

    public Configuration(String fileName, int nodeNumber) throws ParseException, FileNotFoundException {
        this.nodeNumber = nodeNumber - 1;
        Scanner scanner = new Scanner(new File(fileName));
        Map<String, String> properties = new HashMap<>();

        while (scanner.hasNext()) {
            String property = scanner.nextLine();
            int pos = property.indexOf('=');

            if (pos == -1) {
                throw new ParseException("Can't read property: " + property);
            }

            properties.put(property.substring(0, pos), property.substring(pos + 1));
        }

        try {
            serversCount = getNumber(properties, "servers_count");
            timeout = getNumber(properties, "timeout");
            hosts = new ArrayList<>();
            ports = new ArrayList<>();

            for (int i = 0; i < serversCount; i++) {
                String address = get(properties, ("node." + (i + 1)));
                int pos = address.indexOf(":");

                if (pos == -1) {
                    throw new ParseException("Can't read address for server " + i + "!");
                }

                hosts.add(address.substring(0, pos));
                ports.add(Integer.parseInt(address.substring(pos + 1)));
            }
        } catch (NumberFormatException e) {
            throw new ParseException("Can't read number!");
        }
    }

    public int getServersCount() {
        return serversCount;
    }

    public int getNodeNumber() {
        return nodeNumber;
    }

    public int getTimeout() {
        return timeout;
    }

    public List<String> getHosts() {
        return hosts;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    private int getNumber(Map<String, String> properties, String key) throws NumberFormatException, ParseException {
        return Integer.parseInt(get(properties, key));
    }

    private String get(Map<String, String> properties, String key) throws ParseException {
        if (!properties.containsKey(key)) {
            throw new ParseException("Unknown property: " + key);
        }

        return properties.get(key);
    }
}
