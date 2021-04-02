package com.vanlightly.toy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

public class EchoServer {
    String nodeId;
    int nextMsgId;
    ObjectMapper mapper;
    AtomicBoolean stopSig;

    public EchoServer(AtomicBoolean stopSig) {
        this.stopSig = stopSig;
        nextMsgId = 0;
        mapper = new ObjectMapper();
    }

    public void run() {
        Scanner scanner = new Scanner(System.in);
        System.err.println("Waiting for input");

        while(!stopSig.get()) {
            String input = scanner.nextLine();
            try {
                JsonNode req = mapper.readTree(input);
                System.err.println("Received: " + req);

                String type = req.get("body").get("type").asText();

                switch(type) {
                    case "init":
                        initialize(req);
                        break;
                    case "echo":
                        echo(req);
                        break;
                }
            } catch(JsonMappingException e) {
                e.printStackTrace(System.err);
            } catch(JsonProcessingException e) {
                e.printStackTrace(System.err);
            } catch(Exception e) {
                e.printStackTrace(System.err);
            }
        }
    }

    private void initialize(JsonNode req) {
        nodeId = req.get("body").get("node_id").asText();
        System.err.println("Initialized node " + nodeId);

        ObjectNode replyBody = mapper.createObjectNode();
        replyBody.put("type", "init_ok");

        reply(req, replyBody);
    }

    private void echo(JsonNode req) {
        System.err.println("Echoing " + req.get("body"));

        ObjectNode replyBody = (ObjectNode)req.get("body");
        replyBody.put("type", "echo_ok");

        reply(req, replyBody);
    }

    private void reply(JsonNode req, JsonNode replyBody) {
        nextMsgId++;
        ObjectNode newBody = replyBody.deepCopy();
        newBody.put("msg_id", nextMsgId);
        newBody.set("in_reply_to", req.get("body").get("msg_id"));
        ObjectNode msg = mapper.createObjectNode();
        msg.put("src", nodeId);
        msg.put("dest", req.get("src").asText());
        msg.set("body", newBody);

        System.out.println(msg.toString());
        System.out.flush();
    }
}
