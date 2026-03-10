package com.chatflow.server.model;

public class QueueMessage {

    private String messageId;
    private String roomId;
    private String userId;
    private String username;
    private String message;
    private String timestamp;
    private String messageType;
    private String serverId;
    private String clientIp;
    private long sendTime;  // Timestamp when client sent the message

    public QueueMessage() {}

    public QueueMessage(ChatMessage src, String messageId,
                        String roomId, String serverId, String clientIp) {
        this.messageId   = messageId;
        this.roomId      = roomId;
        this.userId      = src.getUserId();
        this.username    = src.getUsername();
        this.message     = src.getMessage();
        this.timestamp   = src.getTimestamp();
        this.messageType = src.getMessageType();
        this.serverId    = serverId;
        this.clientIp    = clientIp;
        this.sendTime    = src.getSendTime();
    }

    // Getters & Setters
    public String getMessageId()  { return messageId; }
    public String getRoomId()     { return roomId; }
    public String getUserId()     { return userId; }
    public String getUsername()   { return username; }
    public String getMessage()    { return message; }
    public String getTimestamp()  { return timestamp; }
    public String getMessageType(){ return messageType; }
    public String getServerId()   { return serverId; }
    public String getClientIp()   { return clientIp; }

    public void setMessageId(String v)  { this.messageId = v; }
    public void setRoomId(String v)     { this.roomId = v; }
    public void setUserId(String v)     { this.userId = v; }
    public void setUsername(String v)   { this.username = v; }
    public void setMessage(String v)    { this.message = v; }
    public void setTimestamp(String v)  { this.timestamp = v; }
    public void setMessageType(String v){ this.messageType = v; }
    public void setServerId(String v)   { this.serverId = v; }
    public void setClientIp(String v)   { this.clientIp = v; }
    
    public long getSendTime() { return sendTime; }
    public void setSendTime(long v) { this.sendTime = v; }
}