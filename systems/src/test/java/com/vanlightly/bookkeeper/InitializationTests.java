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

//    @Test
//    public void testBookieSession() {
//        AtomicBoolean isCancelled = new AtomicBoolean();
//        NetworkRouter router = new NetworkRouter(isCancelled);
//        int nodeCount = 5;
//
//        for (int n = 1; n <= nodeCount; n++) {
//            router.addNode("n" + n);
//            String init = "{\"dest\":\"n" + n +"\",\"body\":{\"type\":\"init\",\"node_id\":\"n" + n +"\",\"msg_id\":" + n + "},\"src\":\"c1\"}\n";
//            router.send("n" + n, init);
//        }
//
//        router.routeMessages();
//
//        try {
//            Thread.sleep(100000000);
//        } catch (Exception e) {
//
//        }
//    }
}
