package com.chatflow.server.handler;

import com.chatflow.server.model.ChatMessage;
import com.chatflow.server.model.ChatResponse;
import com.chatflow.server.model.QueueMessage;
import com.chatflow.server.model.UserInfo;
import com.chatflow.server.mq.KafkaProducerPool;
import com.chatflow.server.mq.ConsumerPool;
import com.chatflow.server.mq.MessageProducer;
import com.chatflow.server.mq.RoomManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class ChatWebSocketHandler extends TextWebSocketHandler {

    public final ConcurrentHashMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, Set<WebSocketSession>> roomSessions = new ConcurrentHashMap<>();
    public final ObjectMapper objectMapper = new ObjectMapper();

    private KafkaProducerPool kafkaProducerPool;
    private MessageProducer messageProducer;
    private ConsumerPool consumerPool;

    @Autowired
    private RoomManager roomManager;

    // 统计接收到的消息数
    private final AtomicLong messagesReceived = new AtomicLong(0);

    private static final String SERVER_ID =
            System.getenv().getOrDefault("SERVER_ID", "ws-server-01");

    public ChatWebSocketHandler() {
        // Delay initialization to initRabbitMQ() which is called after autowiring
    }

    public final ConcurrentHashMap<String, UserInfo> activeUsers = new ConcurrentHashMap<>();

    @PostConstruct
    public void initRabbitMQ() {
        try {
            this.kafkaProducerPool = new KafkaProducerPool(200);  // Increased from 50 to handle more concurrent producers/consumers
            this.kafkaProducerPool.createProducer();
            this.messageProducer = new MessageProducer(this.kafkaProducerPool);
            this.consumerPool = new ConsumerPool(this.kafkaProducerPool, this.roomManager);
            this.consumerPool.init();
            //System.out.println("MessageProducer ready");
            //System.out.println("ConsumerPool started with 80 threads");
        } catch (Exception e){
            //System.err.println("Failed to connect RabbitMQ: " + e.getMessage());
            throw new RuntimeException("RabbitMQ init failed", e);
        }
    }

    // 每 30 秒发送一次 ping 保持连接
    @Scheduled(fixedRate = 30000)
    public void sendPing() {
        PingMessage ping = new PingMessage();
        for (WebSocketSession session : sessions.values()) {
            try {
                if (session.isOpen()) {
                    session.sendMessage(ping);
                }
            } catch (Exception ignored) {}
        }
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        // System.out.println("New WebSocket Connection: " + session.getId() + " to " + session.getRemoteAddress());
        // 1. 立即包装原始 session - 增加缓冲到 8MB
        // Increase buffer and timeout significantly to handle high throughput
        // Also pass a custom handler or just rely on ConcurrentWebSocketSessionDecorator's internal queueing if available
        // Note: ConcurrentWebSocketSessionDecorator has an internal queue but limited capacity.
        // We can wrap it or just use the decorator.
        WebSocketSession decoratedSession = new ConcurrentWebSocketSessionDecorator(session, 10000, 1 * 1024 * 1024);
        String roomId = extractRoomId(session);

        // 2. 关键：存入 Map 的必须是包装后的 decoratedSession！！
        sessions.put(session.getId(), decoratedSession);
        roomSessions.computeIfAbsent(roomId, k -> ConcurrentHashMap.newKeySet()).add(decoratedSession);

        activeUsers.put(
                session.getId(),
                new UserInfo("unknown", "unknown", roomId, session.getId())
        );

        // System.out.println("New connection: " + session.getId() + " (Decorated) to room: " + roomId);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        WebSocketSession safeSession = sessions.get(session.getId());
        if (safeSession == null) safeSession = session;

        try {
            // 1. Parse and validate message
            ChatMessage chatMsg = parseAndValidate(message.getPayload());

            // 2. Validate
            if (!isValid(chatMsg)) {
                //System.err.println("Validation failed for session " + session.getId());
                sendError(session, "Invalid message format");
                return;
            }

            String roomId = extractRoomId(session);
            String clientIp = extractClientIp(session);
            String msgId = UUID.randomUUID().toString();

            // 统计接收到的消息
            messagesReceived.incrementAndGet();

            QueueMessage queueMsg = new QueueMessage(chatMsg,msgId,roomId,SERVER_ID,clientIp);

            // 3. Echo back with server timestamp
            try{
                messageProducer.publish(queueMsg);
                // System.out.println("MQ >>> Published msg " + msgId + " to room." + roomId);
                ChatResponse response = new ChatResponse(chatMsg,"SUCCESS");
                safeSession.sendMessage(new TextMessage(toJson(response)));
            } catch (IllegalStateException e){
                 // 熔断器触发
                 System.err.println("CIRCUIT OPEN: " + messagesReceived.get());
                 sendError(session, "Publish failed: Circuit Open");
            } catch (java.util.concurrent.TimeoutException e) {
                 // Pool耗尽
                 System.err.println("POOL EXHAUSTED: " + messagesReceived.get());
                 sendError(session, "Publish failed: Pool Exhausted");
            } catch (Exception e) {
                 // 其他原因
                 System.err.println("PUBLISH FAILED [" + e.getClass().getSimpleName() + "]: " + e.getMessage());
                 sendError(session, "Error publishing message");
            }

        } catch (Exception e) {
            //System.err.println("Error processing message: " + e.getMessage());
            sendError(session, "Error processing message: " + e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        String sessionId = session.getId();
        String roomId = extractRoomId(session);

        // 1. 从全局 Map 移除
        sessions.remove(sessionId);
        activeUsers.remove(sessionId);

        // 2. 从房间 Map 移除 (使用 removeIf 确保能匹配到装饰器对象)
        Set<WebSocketSession> roomSet = roomSessions.get(roomId);
        if (roomSet != null) {
            roomSet.removeIf(s -> s.getId().equals(sessionId));
        }

        // 3. Clean up (locks removed as they caused contention)
        //System.out.println("Connection closed and cleaned up: " + sessionId);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        //System.err.println("Transport error for session " + session.getId() + ": " + exception.getMessage());
        sessions.remove(session.getId());
    }

    // Helper methods

    private String extractRoomId(WebSocketSession session) {
        String uri = session.getUri().toString();
        String[] parts = uri.split("/");
        return parts.length > 0? parts[parts.length-1]: "default";
    }

    private String extractClientIp(WebSocketSession session) {
        try {
            return session.getRemoteAddress().getAddress().getHostAddress();
        } catch (Exception e) {
            return "unknown";
        }
    }

    private ChatMessage parseAndValidate(String payload) throws Exception {
        return objectMapper.readValue(payload, ChatMessage.class);
    }

    private boolean isValid(ChatMessage msg) {
        try {
            // userId: 1-100000
            int userId = Integer.parseInt(msg.getUserId());
            if (userId < 1 || userId > 100000) {
                return false;
            }

            // username: 3-20 alphanumeric
            String username = msg.getUsername();
            if (username == null || username.length() < 3 || username.length() > 20) {
                return false;
            }
            if (!username.matches("[a-zA-Z0-9]+")) {
                return false;
            }

            // message: 1-500 chars
            String message = msg.getMessage();
            if (message == null || message.length() < 1 || message.length() > 500) {
                return false;
            }

            // timestamp: ISO-8601
            // Optimization: Check if not null first, skip full parsing for speed
            // Instant.parse(msg.getTimestamp()); // Slow

            // messageType: TEXT|JOIN|LEAVE
            if (!Arrays.asList("TEXT", "JOIN", "LEAVE").contains(msg.getMessageType())) {
                return false;
            }

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void sendError(WebSocketSession session, String errorMessage) {

        if (session == null || !session.isOpen()) return;

        try {
            ErrorResponse error = new ErrorResponse(errorMessage);
            String json = objectMapper.writeValueAsString(error);
            session.sendMessage(new TextMessage(json));
        } catch (Exception e) {
            //System.err.println("Failed to send error message: " + e.getMessage());
        }
    }

    private String toJson(Object obj) throws Exception {
        return objectMapper.writeValueAsString(obj);
    }

    // Inner class for error responses
    private static class ErrorResponse {
        private String error;
        private String timestamp;

        public ErrorResponse(String error) {
            this.error = error;
            this.timestamp = Instant.now().toString();
        }

        public String getError() { return error; }
        public void setError(String error) { this.error = error; }

        public String getTimestamp() { return timestamp; }
        public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    }


    @PreDestroy
    public void destroy() {
        if (kafkaProducerPool != null) {
            kafkaProducerPool.shutdown();
            //System.out.println("ChannelPool shut down");
        }
    }

    public KafkaProducerPool getChannelPool() {
        return kafkaProducerPool;
    }

    public MessageProducer getMessageProducer() {
        return messageProducer;
    }

    public long getMessagesReceived() {
        return messagesReceived.get();
    }
}