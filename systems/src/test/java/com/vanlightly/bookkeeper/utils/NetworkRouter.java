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

//    public boolean waitForInitOk(int nodeCount) {
//        int initialized = 0;
//
//        while (!isCancelled.get()) {
//            if (!msgQueue.isEmpty()) {
//                String msgStr = msgQueue.poll();
//                if (msgStr.contains("init_ok")) {
//                    initialized++;
//                } else {
//
//                }
//            }
//        }
//    }

    public void routeMessages() {
        executorService.submit(() -> {
            try {
                while (!isCancelled.get()) {
                    if (!msgQueue.isEmpty()) {
                        String msgStr = msgQueue.poll();

                        try {
                            JsonNode msg = mapper.readTree(msgStr);
                            String dest = msg.get("dest").asText();
                            String source = msg.get("src").asText();
                            String type = msg.path("body").path("type").asText();

                            String reqOrReply = "request";
                            if (msg.path("body").has("in_reply_to")) {
                                reqOrReply = "reply";
                            }

                            System.out.println("(" + source + ")-[" + type + "]->(" + dest + ") " + reqOrReply);

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
