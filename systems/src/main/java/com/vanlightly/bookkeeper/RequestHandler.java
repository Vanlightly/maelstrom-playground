package com.vanlightly.bookkeeper;

import com.fasterxml.jackson.databind.JsonNode;

public interface RequestHandler {
    boolean handlesRequest(String requestType);
    void handleRequest(JsonNode request);
}
