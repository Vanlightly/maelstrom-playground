package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.CompletableFuture;

public interface MessageSender {
    void send(String destId, String command);
    void send(String destId, String command, ObjectNode body);
    CompletableFuture<JsonNode> sendRequest(String destId, String command);
    CompletableFuture<JsonNode> sendRequest(String destId, String command, ObjectNode body);
    CompletableFuture<JsonNode> sendRequest(String destId, String command, ObjectNode body, int timeoutMs);
}
