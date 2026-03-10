package com.chatflow.server.model;

public class UserInfo {
    private String userId;
    private String username;
    private String roomId;
    private String sessionId;
    private long connectedAt;

    public UserInfo(String userId, String username, String roomId, String sessionId) {
        this.userId = userId;
        this.username = username;
        this.roomId = roomId;
        this.sessionId = sessionId;
        this.connectedAt = System.currentTimeMillis();
    }

    // Getters
    public String getUserId() { return userId; }
    public String getUsername() { return username; }
    public String getRoomId() { return roomId; }
    public String getSessionId() { return sessionId; }
    public long getConnectedAt() { return connectedAt; }
}