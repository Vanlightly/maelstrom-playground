package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.vanlightly.bookkeeper.utils.NetworkRouter;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class InitializationTests {
    ObjectMapper mapper;

    public InitializationTests() {
        mapper = new ObjectMapper();
    }

    @Test
    public void testNoKvRequests() {
        AtomicBoolean isCancelled = new AtomicBoolean();
        NetworkRouter router = new NetworkRouter(isCancelled);
        int nodeCount = 6;

        for (int n = 1; n <= nodeCount; n++) {
            String nodeId = "n" + n;
            router.addNode(nodeId);
            String init = "{\"dest\":\"" + nodeId +"\",\"body\":{\"type\":\"init\",\"node_id\":\"" + nodeId +"\",\"msg_id\":" + n + "},\"src\":\"c1\"}\n";
            router.send(nodeId, init);
        }

        router.routeMessages();

        try {
            Thread.sleep(100000000);
        } catch (Exception e) {

        }
    }

//    @Test
//    public void testOneKvWrite() {
//        AtomicBoolean isCancelled = new AtomicBoolean();
//        NetworkRouter router = new NetworkRouter(isCancelled);
//        int nodeCount = 6;
//
//        for (int n = 1; n <= nodeCount; n++) {
//            String nodeId = "n" + n;
//            router.addNode(nodeId);
//            String init = "{\"dest\":\"" + nodeId +"\",\"body\":{\"type\":\"init\",\"node_id\":\"" + nodeId +"\",\"msg_id\":" + n + "},\"src\":\"c1\"}\n";
//            router.send(nodeId, init);
//        }
//
//        router.routeMessages();
//
//        try {
//            String write = "{\"dest\":\"" + nodeId +"\",\"body\":{\"type\":\"init\",\"node_id\":\"" + nodeId +"\",\"msg_id\":" + n + "},\"src\":\"c1\"}\n";
//
//        } catch (Exception e) {
//
//        }
//    }
}
