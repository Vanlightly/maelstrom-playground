package com.vanlightly.bookkeeper.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.Commands;
import com.vanlightly.bookkeeper.Constants;
import com.vanlightly.bookkeeper.Fields;
import com.vanlightly.bookkeeper.NodeRunner;
import com.vanlightly.bookkeeper.network.NetworkIO;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

public class NetworkRouter {
    Map<String, TestNetworkIO> nodeNetworkIo;
    Map<String, ExecutorService> nodeRunners;
    BlockingQueue<String> msgQueue;
    ExecutorService executorService;
    AtomicBoolean isCancelled;
    ObjectMapper mapper;
    Consumer<JsonNode> msgPrinter;

    public NetworkRouter(AtomicBoolean isCancelled,
                         Consumer<JsonNode> msgPrinter) {
        this.msgPrinter = msgPrinter;
        this.msgQueue = new ArrayBlockingQueue<>(10000);
        this.nodeNetworkIo = new HashMap<>();
        this.nodeRunners = new HashMap<>();
        this.isCancelled = isCancelled;
        this.executorService = Executors.newSingleThreadExecutor();
        this.mapper = new ObjectMapper();
    }

    public void addNode(String nodeId) {
        TestNetworkIO net = new TestNetworkIO(nodeId, msgQueue);
        nodeNetworkIo.put(nodeId, net);
        ExecutorService runnerExecutor = Executors.newSingleThreadExecutor(new NodeThreadFactory(nodeId));
        runnerExecutor.submit(() -> {
            try {
                NodeRunner nodeRunner = new NodeRunner(net);
                nodeRunner.run();
            } catch(Exception e) {
                System.out.println("Node " + nodeId + " has exited with an error");
                e.printStackTrace();
            }
        });
        nodeRunners.put(nodeId, runnerExecutor);
    }

    public void partitionNode(String nodeId, boolean incoming, boolean outgoing) {
        TestNetworkIO net = nodeNetworkIo.get(nodeId);
        if (net != null) {
            net.setLoseIncoming(incoming);
            net.setLoseOutgoing(outgoing);
            System.out.println("Partitioning " + nodeId + " in=" + incoming + " out=" + outgoing);
        } else {
            System.out.println("Cannot partition " + nodeId + " as it does not exist");
        }
    }

    public void healNode(String nodeId) {
        TestNetworkIO net = nodeNetworkIo.get(nodeId);
        if (net != null) {
            net.loseNone();
            System.out.println("Healing " + nodeId);
        } else {
            System.out.println("Cannot heal " + nodeId + " as it does not exist");
        }
    }

    public boolean isPartitioned(String nodeId) {
        TestNetworkIO net = nodeNetworkIo.get(nodeId);
        if (net != null) {
            return net.losesIncoming() || net.losesOutgoing();
        } else {
            return false;
        }
    }

    public void routeMessages() {
        executorService.submit(() -> {
            try {
                while (!isCancelled.get()) {
                    if (!msgQueue.isEmpty()) {
                        String msgStr = msgQueue.poll();

                        try {
                            JsonNode msg = mapper.readTree(msgStr);
                            String dest = msg.get("dest").asText();

                            msgPrinter.accept(msg);

                            if (nodeNetworkIo.containsKey(dest)) {
                                nodeNetworkIo.get(dest).route(msgStr);
                            }
                        } catch (Exception e) {
                            System.out.println("Error with msg: " + msgStr);
                            throw e;
                        }
                    } else {
                        Thread.sleep(10);
                    }
                }
            } catch(Exception e) {
                System.out.println("Message router terminated with error");
                e.printStackTrace();
            }
        });
    }

    public void send(String dest, String msgStr) {
        nodeNetworkIo.get(dest).route(msgStr);
    }

    private static class NodeThreadFactory implements ThreadFactory {
        private String nodeId;

        public NodeThreadFactory(String nodeId) {
            this.nodeId = nodeId;
        }

        public Thread newThread(Runnable r) {
            return new Thread(r, nodeId);
        }
    }
}
