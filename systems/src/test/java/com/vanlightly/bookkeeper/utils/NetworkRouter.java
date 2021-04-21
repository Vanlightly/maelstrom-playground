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
        TestNetworkIO net = new TestNetworkIO(nodeId, msgQueue, mapper);
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

    public void partitionNode(String nodeId) {
        TestNetworkIO net = nodeNetworkIo.get(nodeId);
        if (net != null) {
            for (String node : nodeRunners.keySet()) {
                if (!nodeId.equals(node)) {
                    net.addBlockedNode(node);

                    TestNetworkIO otherNet = nodeNetworkIo.get(node);
                    otherNet.addBlockedNode(nodeId);
                }
            }
            System.out.println("Partitioning " + nodeId);
        } else {
            System.out.println("Cannot partition " + nodeId + " as it does not exist");
        }
    }

    public void partitionNodes(String node1, String node2) {
        partitionNodeLink(node1, node2);
        partitionNodeLink(node2, node1);
    }

    public void partitionNodeLink(String node1, String node2) {
        TestNetworkIO net = nodeNetworkIo.get(node1);
        if (net != null) {
            net.addBlockedNode(node2);
        } else {
            System.out.println("Cannot partition " + node1 + " as it does not exist");
        }
    }

    public void healNode(String nodeId) {
        TestNetworkIO net = nodeNetworkIo.get(nodeId);
        if (net != null) {
            net.clearBlockedNodes();

            for (String node : nodeRunners.keySet()) {
                if (!nodeId.equals(node)) {
                    TestNetworkIO otherNet = nodeNetworkIo.get(node);
                    otherNet.removeBlockedNode(nodeId);
                }
            }

            System.out.println("Healing " + nodeId);
        } else {
            System.out.println("Cannot heal " + nodeId + " as it does not exist");
        }
    }

    public void healNodes(String node1, String node2) {
        healNodeLink(node1, node2);
        healNodeLink(node2, node1);
    }

    public void healNodeLink(String node1, String node2) {
        TestNetworkIO net = nodeNetworkIo.get(node1);
        if (net != null) {
            net.removeBlockedNode(node2);
        } else {
            System.out.println("Cannot heal network of " + node1 + " as it does not exist");
        }
    }

    public boolean isPartitioned(String nodeId) {
        TestNetworkIO net = nodeNetworkIo.get(nodeId);
        if (net != null) {
            return !net.getBlockedNodes().isEmpty();
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
