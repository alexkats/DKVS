package com.katsman.dkvs.server.workers;

import com.katsman.dkvs.common.messages.Entry;
import com.katsman.dkvs.common.messages.Message;
import com.katsman.dkvs.common.messages.csp.ClientServerRequest;
import com.katsman.dkvs.common.messages.csp.ClientServerResponse;
import com.katsman.dkvs.common.messages.ssp.AppendEntriesRequest;
import com.katsman.dkvs.common.messages.ssp.AppendEntriesResult;
import com.katsman.dkvs.common.messages.ssp.VoteRequest;
import com.katsman.dkvs.common.messages.ssp.VoteResponse;
import com.katsman.dkvs.common.utils.ParseException;
import com.katsman.dkvs.server.utils.Configuration;
import com.katsman.dkvs.server.utils.ConnectionFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

/**
 * @author Alexey Katsman
 * @since 14.06.16
 */

public class Server extends Thread {
    private final ReplicatedStateMachine rsm;
    private final Configuration configuration;
    private final ConnectionFactory factory;
    private final Queue<Message> requests;
    private final Queue<Message> responses;
    private final ServerState state;

    private volatile boolean active;

    public Server(Configuration configuration) throws ParseException, IOException {
        rsm = new ReplicatedStateMachine(configuration);
        this.configuration = configuration;
        factory = new ConnectionFactory(configuration, this);
        requests = new ArrayDeque<>();
        responses = new ArrayDeque<>();
        state = new ServerState();
        state.commitIndex = rsm.size();

        if (configuration.getNodeNumber() == 0) {
            state.state = State.LEADER;
        }

        active = true;
    }

    public void add(Message message) {
        synchronized (requests) {
            requests.add(message);
            requests.notify();
        }
    }

    public Message get() throws InterruptedException {
        while (true) {
            synchronized (responses) {
                if (!responses.isEmpty()) {
                    return responses.poll();
                }

                responses.wait();
            }
        }
    }

    @Override
    public void run() {
        factory.start();
        state.lastMessageTime = System.currentTimeMillis();

        while (active) {
            try {
                Message message = getMessage();

                if (message == null) {
                    processTimeout();
                } else {
                    process(message);
                }
            } catch (InterruptedException | ParseException | IOException e) {
                try {
                    close();
                } catch (IOException e1) {
                    System.out.println("Can't close Replicated State Machine!");
                }

                return;
            }
        }
    }

    private void processTimeout() throws IOException {
        switch (state.state) {
            case LEADER:
                for (int i = 0; i < configuration.getServersCount(); i++) {
                    if (i == configuration.getNodeNumber()) {
                        continue;
                    }

                    List<Message> logEntries = new ArrayList<>();

                    for (int j = 0; j < (rsm.size() - state.nextLogIndex.get(i)); j++) {
                        logEntries.add(rsm.getLogEntry(state.nextLogIndex.get(i) + j));
                    }

                    SocketAddress address = new InetSocketAddress(configuration.getHosts().get(i), configuration.getPorts().get(i));
                    int prevLogTerm = -1;

                    if (state.nextLogIndex.get(i) != 0) {
                        Message entry = rsm.getLogEntry(state.nextLogIndex.get(i) - 1);
                        if (entry instanceof Entry) {
                            prevLogTerm = ((Entry) entry).getTerm();
                        }
                    }

                    printAndSend(new AppendEntriesRequest(
                            address,
                            state.term,
                            configuration.getNodeNumber(),
                            state.nextLogIndex.get(i) - 1,
                            prevLogTerm,
                            logEntries,
                            state.commitIndex
                    ));
                }

                break;
            case FOLLOWER:
            case CANDIDATE:
                state.state = State.CANDIDATE;
                state.leader = -1;
                state.setTerm(state.term + 1);
                state.setVotedFor(configuration.getNodeNumber());
                state.votesCount = 1;

                if (configuration.getServersCount() == 1) {
                    state.state = State.LEADER;
                    state.nextLogIndex.set(0, rsm.size());
                    state.matchLogIndex.set(0, 0);
                }

                for (int i = 0; i < configuration.getServersCount(); i++) {
                    if (i == configuration.getNodeNumber()) {
                        continue;
                    }

                    SocketAddress address = new InetSocketAddress(configuration.getHosts().get(i), configuration.getPorts().get(i));
                    int lastLogTerm = -1;

                    if (rsm.size() != 0) {
                        Message entry = rsm.getLogEntry(rsm.size() - 1);

                        if (entry instanceof Entry) {
                            lastLogTerm = ((Entry) entry).getTerm();
                        }
                    }

                    printAndSend(new VoteRequest(
                            address,
                            state.term,
                            configuration.getNodeNumber(),
                            rsm.size() - 1,
                            lastLogTerm
                    ));
                }
        }

        state.lastMessageTime = System.currentTimeMillis();
    }

    private void processClientServerRequest(ClientServerRequest request) throws ParseException, IOException {
        switch (request.getOperation()) {
            case SET:
            case DELETE:
                if (state.state.equals(State.LEADER)) {
                    rsm.add(new Entry(state.term, request.getOperation(), request.getKey(), request.getValue()), request);
                    toCommit();
                    processTimeout();
                } else if (state.leader != -1) {
                    SocketAddress address = new InetSocketAddress(configuration.getHosts().get(state.leader), configuration.getPorts().get(state.leader));
                    ClientServerRequest localRequest = new ClientServerRequest(
                            address,
                            request.getKey(),
                            request.getValue(),
                            request.getOperation(),
                            request.getRedirections()
                    );
                    localRequest.getRedirections().add((InetSocketAddress) request.getAddress());
                    printAndSend(localRequest);
                } else {
                    printAndSend(new ClientServerResponse(
                            request.getAddress(),
                            request.getOperation(),
                            "Unknown leader",
                            false,
                            request.getRedirections()
                    ));
                }

                return;
            case GET:
                printAndSend(new ClientServerResponse(
                        request.getAddress(),
                        request.getOperation(),
                        rsm.get(request.getKey()),
                        rsm.containsKey(request.getKey()),
                        request.getRedirections()
                ));
                return;
            case PING:
                printAndSend(new ClientServerResponse(
                        request.getAddress(),
                        request.getOperation(),
                        null,
                        true,
                        request.getRedirections()
                ));
                return;
            default:
                throw new AssertionError();
        }
    }

    private void processClientServerResponse(ClientServerResponse response) {
        ClientServerResponse localResponse = new ClientServerResponse(
                response.getRedirections().get(response.getRedirections().size() - 1),
                response.getOperation(),
                response.getResult(),
                response.isSuccess(),
                response.getRedirections()
        );
        localResponse.getRedirections().remove(localResponse.getRedirections().size() - 1);
        printAndSend(localResponse);
    }

    private void processAppendEntriesRequest(AppendEntriesRequest request) throws ParseException, IOException {
        if (request.getTerm() >= state.term) {
            state.state = State.FOLLOWER;
            state.setTerm(request.getTerm());
            state.leader = request.getLeaderId();
            state.lastMessageTime = System.currentTimeMillis();
        }

        if (request.getTerm() == state.term && (request.getPrevLogIndex() == -1 || (rsm.size() > request.getPrevLogIndex() && ((Entry) rsm.getLogEntry(request.getPrevLogIndex())).getTerm() == request.getPrevLogTerm()))) {
            rsm.removeAfter(request.getPrevLogIndex());
            rsm.restore(request.getEntries());

            if (request.getLeaderCommit() > state.commitIndex) {
                commit(Math.min(request.getLeaderCommit(), rsm.size()));
            }

            printAndSend(new AppendEntriesResult(request.getAddress(), state.term, true, rsm.size(), configuration.getNodeNumber()));
        } else {
            printAndSend(new AppendEntriesResult(request.getAddress(), state.term, false, rsm.size(), configuration.getNodeNumber()));
        }
    }

    private void processAppendEntriesResult(AppendEntriesResult result) throws ParseException, IOException {
        if (result.isSuccess()) {
            state.nextLogIndex.set(result.getId(), result.getLength());
            state.matchLogIndex.set(result.getId(), result.getLength());
            toCommit();
        } else {
            state.nextLogIndex.set(result.getId(), state.nextLogIndex.get(result.getId()) - 1);
        }
    }

    private void processVoteRequest(VoteRequest request) throws IOException {
        if (request.getTerm() < state.term) {
            printAndSend(new VoteResponse(request.getAddress(), state.term, false));
        } else {
            if (request.getTerm() > state.term) {
                state.state = State.FOLLOWER;
                state.setTerm(request.getTerm());
                state.setVotedFor(-1);
            }

            int lastLogTerm = rsm.size() == 0 ? -2 : ((Entry) rsm.getLogEntry(rsm.size() - 1)).getTerm();
            boolean newest = request.getLastLogTerm() > lastLogTerm || (request.getLastLogTerm() == lastLogTerm && request.getLastLogIndex() >= rsm.size() - 1);

            if (newest && (state.votedFor == -1 || state.votedFor == request.getCandidateId())) {
                state.setVotedFor(request.getCandidateId());
                printAndSend(new VoteResponse(request.getAddress(), state.term, true));
            } else {
                printAndSend(new VoteResponse(request.getAddress(), state.term, false));
            }
        }
    }

    private void processVoteResponse(VoteResponse response) throws IOException {
        if (!state.state.equals(State.CANDIDATE)) {
            return;
        }

        if (response.getTerm() == state.term && response.isVoteGranted()) {
            state.votesCount++;

            if (state.votesCount > configuration.getServersCount() / 2) {
                state.state = State.LEADER;

                for (int i = 0; i < state.nextLogIndex.size(); i++) {
                    state.nextLogIndex.set(i, rsm.size());
                    state.matchLogIndex.set(i, 0);
                }

                processTimeout();
            }
        } else if (response.getTerm() > state.term) {
            state.setTerm(response.getTerm());
            state.setVotedFor(-1);
            state.state = State.FOLLOWER;
        }
    }

    private void process(Message message) throws ParseException, IOException {
        System.out.println(getStatus() + " Message received (" + message.getAddress() + "): " + message.print());

        if (message instanceof ClientServerRequest) {
            processClientServerRequest((ClientServerRequest) message);
        } else if (message instanceof ClientServerResponse) {
            processClientServerResponse((ClientServerResponse) message);
        } else if (message instanceof AppendEntriesRequest) {
            processAppendEntriesRequest((AppendEntriesRequest) message);
        } else if (message instanceof AppendEntriesResult) {
            processAppendEntriesResult((AppendEntriesResult) message);
        } else if (message instanceof VoteRequest) {
            processVoteRequest((VoteRequest) message);
        } else if (message instanceof VoteResponse) {
            processVoteResponse((VoteResponse) message);
        } else {
            throw new AssertionError();
        }
    }

    private Message getMessage() throws InterruptedException {
        while (true) {
            synchronized (requests) {
                if (!requests.isEmpty()) {
                    return requests.poll();
                } else if ((System.currentTimeMillis() - state.lastMessageTime) >= getTimeout()) {
                    return null;
                } else {
                    requests.wait(getTimeout() - (System.currentTimeMillis() - state.lastMessageTime));
                }
            }
        }
    }

    private void printAndSend(Message message) {
        System.out.println(getStatus() + " Sending message (" + message.getAddress() + "): " + message.print());

        synchronized (responses) {
            responses.add(message);
            responses.notify();
        }
    }

    private String getStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        sb.append(state.state.name());
        sb.append("; term = ");
        sb.append(state.term);
        sb.append("]");
        return sb.toString();
    }

    private int getTimeout() {
        if (state.state.equals(State.LEADER)) {
            return configuration.getTimeout() / 2;
        } else {
            return configuration.getTimeout();
        }
    }

    private void toCommit() throws ParseException, IOException {
        for (int i = state.commitIndex; i < rsm.size(); i++) {
            int count = 1;

            for (int j = 0; j < configuration.getServersCount(); j++) {
                if (j != configuration.getNodeNumber() && state.matchLogIndex.get(j) > i) {
                    count++;
                }
            }

            Message message = rsm.getLogEntry(i);
            Entry entry;

            if (message instanceof Entry) {
                entry = (Entry) message;
            } else {
                throw new AssertionError();
            }

            if (count > configuration.getServersCount() / 2 && entry.getTerm() == state.term) {
                commit(i + 1);
            }
        }
    }

    private void commit(int commitIndex) throws ParseException, IOException {
        List<ClientServerResponse> responses = state.commit(commitIndex);
        responses.forEach(this::printAndSend);
    }

    private void close() throws IOException {
        active = false;
        factory.close();
        rsm.close();
        interrupt();
    }

    private class ServerState {
        State state;
        int term;
        int votedFor;
        int votesCount;
        int leader;
        int commitIndex;
        long lastMessageTime;
        List<Integer> nextLogIndex;
        List<Integer> matchLogIndex;

        private final String serverStateFileName;

        ServerState() throws IOException {
            leader = -1;
            serverStateFileName = "dkvs_" + (configuration.getNodeNumber() + 1) + ".state";
            nextLogIndex = new ArrayList<>();
            matchLogIndex = new ArrayList<>();

            for (int i = 0; i < configuration.getServersCount(); i++) {
                nextLogIndex.add(rsm.size());
                matchLogIndex.add(0);
            }

            try {
                Scanner scanner = new Scanner(new File(serverStateFileName));
                term = scanner.nextInt();
                votedFor = scanner.nextInt();
                scanner.close();
            } catch (FileNotFoundException e) {
                votedFor = -1;
            }
        }

        void setTerm(int term) throws IOException {
            this.term = term;
            update();
        }

        void setVotedFor(int votedFor) throws IOException {
            this.votedFor = votedFor;
            update();
        }

        void update() throws IOException {
            Writer writer = new FileWriter(new File(serverStateFileName));
            writer.write(term + " " + votedFor);
            writer.close();
        }

        List<ClientServerResponse> commit(int commitIndex) throws ParseException, IOException {
            List<ClientServerResponse> responses = rsm.commit(this.commitIndex, commitIndex);
            this.commitIndex = commitIndex;
            return responses;
        }
    }

    private enum State {
        LEADER,
        CANDIDATE,
        FOLLOWER
    }
}
