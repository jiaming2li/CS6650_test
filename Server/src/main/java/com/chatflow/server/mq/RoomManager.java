package com.chatflow.server.mq;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import com.chatflow.server.model.QueueMessage;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class RoomManager {

    // 删除了限流逻辑，避免阻塞 Consumer 线程导致 Channel 耗尽

    // 移除了 ChatWebSocketHandler 的依赖，改为自行管理 roomSessions
    public final ConcurrentHashMap<String, Set<WebSocketSession>> roomSessions = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper = new ObjectMapper();
    private AtomicLong messageProcessed = new AtomicLong(0);
    private AtomicLong messageFailed = new AtomicLong(0);
    private AtomicLong messageDelivered = new AtomicLong(0);
    private AtomicLong duplicatesSkipped = new AtomicLong(0);

    // 去重 set，使用 ConcurrentHashMap 实现无锁去重
    // 定期清理，防止内存无限增长
    private final ConcurrentHashMap<String, Long> processedMessages = new ConcurrentHashMap<>();

    // 定时清理任务 - 每 5 秒清理一次过期消息
    private static final long CLEANUP_INTERVAL_MS = 5000;
    private static final long MESSAGE_TTL_MS = 10000; // 10 秒 TTL

    public RoomManager() {
        // 启动定时清理线程
        Thread cleanupThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(CLEANUP_INTERVAL_MS);
                    long now = System.currentTimeMillis();
                    processedMessages.entrySet().removeIf(entry -> 
                        now - entry.getValue() > MESSAGE_TTL_MS);
                } catch (InterruptedException ignored) {
                    break;
                }
            }
        });
        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    // 添加 Session 到房间
    public void addSession(String roomId, WebSocketSession session) {
        roomSessions.computeIfAbsent(roomId, k -> ConcurrentHashMap.newKeySet()).add(session);
    }

    // 从房间移除 Session
    public void removeSession(String roomId, WebSocketSession session) {
        Set<WebSocketSession> sessions = roomSessions.get(roomId);
        if (sessions != null) {
            sessions.removeIf(s -> s.getId().equals(session.getId()));
        }
    }

    public void broadcast(String roomId, String rawMessage) {
        // 删除了限流逻辑，避免阻塞 Consumer 线程导致 Channel 耗尽
        // long now = System.currentTimeMillis();
        // if (now - lastSecondResetTime > 1000) {
        //     lastSecondResetTime = now;
        //     lastSecondCounter.set(0);
        // }

        // if (lastSecondCounter.incrementAndGet() > MAX_BROADCAST_PER_SECOND) {
        //     try {
        //         Thread.sleep(5);
        //     } catch (InterruptedException ignored) {}
        // }

        // 直接从自身获取 roomSessions，不再依赖 ChatWebSocketHandler
        if (roomSessions == null || roomSessions.isEmpty()) {
            messageProcessed.incrementAndGet();
            return;
        }

        QueueMessage message = parseMessage(rawMessage);
        if (message == null) return;

        // 去重逻辑：使用 putIfAbsent 实现原子性检查，无需同步锁
        if (processedMessages.putIfAbsent(message.getMessageId(), System.currentTimeMillis()) != null) {
            duplicatesSkipped.incrementAndGet();
            messageProcessed.incrementAndGet();
            return;
        }

        String key = roomId.replace("room.", "");
        // Debug: Verify key matching
        // if (!roomId.equals(key)) System.out.println("DEBUG: Normalized roomId: " + roomId + " -> " + key);
        Set<WebSocketSession> sessions = roomSessions.get(key);
        if (sessions == null || sessions.isEmpty()) {
            // 没有 session 也算处理完成
            messageProcessed.incrementAndGet();
            return;
        }

        // No lock here to allow max parallelism. session.sendMessage is thread-safe.
        // 使用迭代器，可以安全移除
        for (java.util.Iterator<WebSocketSession> it = sessions.iterator(); it.hasNext(); ) {
            WebSocketSession session = it.next();
            if (!session.isOpen()) {
                // 清理断开的 Session，避免重试发空消息
                it.remove();
                continue;
            }
            // ConcurrentWebSocketSessionDecorator is thread-safe, no need to synchronize
            // 它内部有队列，不会阻塞 Consumer 线程
            try {
                session.sendMessage(new TextMessage(rawMessage));
                messageDelivered.incrementAndGet();
            } catch (Exception e) {
                messageFailed.incrementAndGet();
                // 发送失败的也移掉
                it.remove();
                System.err.println("Session send failed (removed): " + session.getId() + " - " + e.getMessage());
            }
        }
        messageProcessed.incrementAndGet();
    }

    private QueueMessage parseMessage(String rawMessage) {
        try {
            return objectMapper.readValue(rawMessage, QueueMessage.class);
        } catch (Exception e) {
            return null;
        }
    }

    public long getMessageProcessed() { return messageProcessed.get(); }
    public long getMessageFailed() { return messageFailed.get(); }
    public long getMessageDelivered() { return messageDelivered.get(); }
    public long getDuplicatesSkipped() { return duplicatesSkipped.get(); }
}
