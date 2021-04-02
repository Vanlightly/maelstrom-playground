package com.vanlightly.bookkeeper.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vanlightly.bookkeeper.NodeRunner;
import com.vanlightly.bookkeeper.network.NetworkIO;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class NetworkRouter {
    Map<String, TestNetworkIO> nodeNetworkIo;
    Map<String, ExecutorService> nodeRunners;
    BlockingQueue<String> msgQueue;
    ExecutorService executorService;
    AtomicBoolean isCancelled;
    ObjectMapper mapper;

    public NetworkRouter(AtomicBoolean isCancelled) {
        this.msgQueue = new ArrayBlockingQueue<>(1000);
        this.nodeNetworkIo = new HashMap<>();
        this.nodeRunners = new HashMap<>();
        this.isCancelled = isCancelled;
        this.executorService = Executors.newSingleThreadExecutor();
        this.mapper = new ObjectMapper();
    }

    public void addNode(String nodeId) {
        TestNetworkIO net = new TestNetworkIO(msgQueue);
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

    public void routeMessages() {
        executorService.submit(() -> {
            try {
                while (!isCancelled.get()) {
                    if (!msgQueue.isEmpty()) {
                        String msgStr = msgQueue.poll();
                        JsonNode msg = mapper.readTree(msgStr);
                        String dest = msg.get("dest").asText();
                        String type = msg.path("body").path("type").asText();
                        if (msg.path("body").has("in_reply_to")) {
                            type = "reply";
                        }

                        if (nodeNetworkIo.containsKey(dest)) {
                            System.out.println("ROUTER: Delivering " + type + " to " + dest + " msg: " + msgStr);
                            nodeNetworkIo.get(dest).route(msgStr);
                        } else {
                            System.out.println("NOT ROUTABLE: To " + dest + " msg: " + msgStr);
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
