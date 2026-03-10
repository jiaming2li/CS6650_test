package com.chatflow.server.controller;

import com.chatflow.server.handler.ChatWebSocketHandler;
import com.chatflow.server.mq.RoomManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

// Health endpoint
@RestController
public class HealthController {

    @Autowired
    private ChatWebSocketHandler wsHandler;

    @Autowired
    private RoomManager roomManager;

    @GetMapping("/health")
    public String health() {

        return "OK";

        //Map<String, Object> status = new HashMap<>();
        //status.put("status", "UP");
        //status.put("activeConnections", wsHandler.sessions.size());
        //status.put("activeRooms", wsHandler.roomSessions.size());
        //status.put("activeUsers", wsHandler.activeUsers.size());
        //status.put("messagesProcessed", roomManager.getMessageProcessed());
        //status.put("messagesFailed", roomManager.getMessageFailed());
        //status.put("messagesDelivered", roomManager.getMessageDelivered());
        //status.put("duplicatesSkipped", roomManager.getDuplicatesSkipped());
        //return status;
    }
}
