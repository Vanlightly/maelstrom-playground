package com.vanlightly.bookkeeper.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.Commands;
import com.vanlightly.bookkeeper.Fields;
import com.vanlightly.bookkeeper.network.NetworkIO;

import java.util.*;
import java.util.concurrent.BlockingQueue;

public class TestNetworkIO implements NetworkIO {
    private ObjectMapper mapper;
    private String nodeId;
    private Queue<String> inQueue;
    private BlockingQueue<String> outQueue;
    private Set<String> blockedNodes;

    public TestNetworkIO(String nodeId,
                         BlockingQueue<String> outQueue,
                         ObjectMapper mapper) {
        this.nodeId = nodeId;
        this.outQueue = outQueue;
        this.mapper = mapper;
        this.inQueue = new ArrayDeque<>();
        this.blockedNodes = new HashSet<>();
    }

    public void addBlockedNode(String node) {
        this.blockedNodes.add(node);
    }

    public void removeBlockedNode(String node) {
        this.blockedNodes.remove(node);
    }

    public void clearBlockedNodes() {
        this.blockedNodes.clear();
    }

    public Set<String> getBlockedNodes() {
        return blockedNodes;
    }

    public void route(String msgStr) {
        JsonNode msg = null;
        try {
            msg = mapper.readTree(msgStr);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        String source = msg.get(Fields.SOURCE).asText();
        String dest = msg.get(Fields.DEST).asText();
        if (!blockedNodes.contains(dest) || msgStr.contains(Commands.PRINT_STATE)) {
            inQueue.add(msgStr);
        } else if (source.equals("c1")) {
            System.out.println("Dropped msg from c1 to node " + nodeId + " Msg: " + msg);
        }
    }

    @Override
    public boolean hasNext() {
        return !inQueue.isEmpty();
    }

    @Override
    public String readNext() {
        return inQueue.poll();
    }

    @Override
    public void write(String msgStr) {
        JsonNode msg = null;
        try {
            msg = mapper.readTree(msgStr);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        String dest = msg.get(Fields.DEST).asText();
        if (!blockedNodes.contains(dest)) {
            outQueue.add(msgStr);
        }
    }
}
