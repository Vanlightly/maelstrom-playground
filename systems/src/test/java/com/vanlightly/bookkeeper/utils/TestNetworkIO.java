package com.vanlightly.bookkeeper.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.NodeRunner;
import com.vanlightly.bookkeeper.network.NetworkIO;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestNetworkIO implements NetworkIO {
    private Queue<String> inQueue;
    private BlockingQueue<String> outQueue;

    public TestNetworkIO(BlockingQueue<String> outQueue) {
        this.outQueue = outQueue;
        this.inQueue = new ArrayDeque<>();
    }

    public void route(String msg) {
        inQueue.add(msg);
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
    public void write(String msg) {
        outQueue.add(msg);
    }
}
