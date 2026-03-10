package com.chatflow.server.model;

import java.time.Instant;

public class ChatResponse {
    private ChatMessage message;
    private String serverTimestamp;
    private String status;

    public ChatResponse(ChatMessage message, String status) {
        this.message = message;
        this.serverTimestamp = Instant.now().toString();
        this.status = status;
    }

    // Getters and Setters
    public ChatMessage getMessage() { return message; }
    public void setMessage(ChatMessage message) { this.message = message; }

    public String getServerTimestamp() { return serverTimestamp; }
    public void setServerTimestamp(String serverTimestamp) {
        this.serverTimestamp = serverTimestamp;
    }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
}
