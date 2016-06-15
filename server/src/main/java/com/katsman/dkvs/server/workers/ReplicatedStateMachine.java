package com.katsman.dkvs.server.workers;

import com.katsman.dkvs.common.messages.Entry;
import com.katsman.dkvs.common.messages.Message;
import com.katsman.dkvs.common.messages.csp.ClientServerRequest;
import com.katsman.dkvs.common.messages.csp.ClientServerResponse;
import com.katsman.dkvs.common.utils.ParseException;
import com.katsman.dkvs.server.utils.Configuration;

import java.io.*;
import java.util.*;

/**
 * @author Alexey Katsman
 * @since 15.06.16
 */

public class ReplicatedStateMachine {
    private final Map<String, String> data;
    private final Map<Integer, ClientServerRequest> requests;
    private final List<Message> logEntries;
    private final Writer writer;

    public ReplicatedStateMachine(Configuration configuration) throws ParseException, IOException {
        data = new HashMap<>();
        requests = new HashMap<>();
        logEntries = new ArrayList<>();
        String logFileName = "dkvs_" + (configuration.getNodeNumber() + 1) + ".log";
        writer = new FileWriter(logFileName, true);
        restore(logFileName);
    }

    public String get(String key) {
        return data.get(key);
    }

    public boolean containsKey(String key) {
        return data.containsKey(key);
    }

    public void restore(List<Message> messages) {
        logEntries.addAll(messages);
    }

    public void add(Message message, ClientServerRequest request) {
        logEntries.add(message);
        requests.put(logEntries.size() - 1, request);
    }

    public Message getLogEntry(int pos) {
        return logEntries.get(pos);
    }

    public void removeAfter(int pos) {
        for (int i = pos + 1; i < logEntries.size(); i++) {
            requests.remove(i);
        }

        int last = logEntries.size() - 1;

        while (last > pos) {
            logEntries.remove(last--);
        }
    }

    public int size() {
        return logEntries.size();
    }

    public void close() throws IOException {
        writer.close();
    }

    public List<ClientServerResponse> commit(int last, int next) throws ParseException, IOException {
        List<ClientServerResponse> responses = new ArrayList<>();

        for (int i = last; i < next; i++) {
            ClientServerResponse response = apply(i);

            if (response != null) {
                responses.add(response);
            }

            writer.write(logEntries.get(i) + "\n");
        }

        writer.flush();
        return responses;
    }

    private void restore(String logFileName) throws ParseException, FileNotFoundException {
        Scanner scanner = new Scanner(new File(logFileName));

        while (scanner.hasNext()) {
            logEntries.add(Message.parse(scanner));
            apply(logEntries.size() - 1);
        }

        scanner.close();
    }

    private ClientServerResponse apply(int operationLogNumber) throws ParseException {
        Message logMessage = logEntries.get(operationLogNumber);

        if (logMessage instanceof Entry) {
            Entry entry = (Entry) logMessage;

            switch (entry.getOperation()) {
                case SET:
                    data.put(entry.getKey(), entry.getValue());

                    if (requests.containsKey(operationLogNumber)) {
                        ClientServerRequest request = requests.get(operationLogNumber);
                        return new ClientServerResponse(request.getAddress(), request.getOperation(), null, true, request.getRedirections());
                    }

                    break;
                case DELETE:
                    boolean success = data.containsKey(entry.getKey());
                    data.remove(entry.getKey());

                    if (requests.containsKey(operationLogNumber)) {
                        ClientServerRequest request = requests.get(operationLogNumber);
                        return new ClientServerResponse(request.getAddress(), request.getOperation(), null, success, request.getRedirections());
                    }

                    break;
                default:
                    throw new ParseException("Log file was corrupted!");
            }

            return null;
        } else {
            throw new ParseException("Log file was corrupted!");
        }
    }
}
