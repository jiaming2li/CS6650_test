package com.chatflow.server.mq;

import com.chatflow.server.handler.ChatWebSocketHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class MetricsCollector {

    @Autowired
    private ObjectProvider<ChatWebSocketHandler> wsHandlerProvider;

    @Autowired
    private RoomManager roomManager;

    private final AtomicLong lastProcessed = new AtomicLong(0);

    @Scheduled(fixedRate = 1000)
    public void logMetrics() {
        try {
            ChatWebSocketHandler wsHandler = wsHandlerProvider.getObject();
            if (wsHandler == null) return;

            // 1. System Metrics
            double cpuLoad = ManagementFactory
                    .getOperatingSystemMXBean().getSystemLoadAverage();
            long memUsed = (Runtime.getRuntime().totalMemory()
                    - Runtime.getRuntime().freeMemory()) / (1024 * 1024);

            // 2. App Metrics
            long currentProcessed = roomManager.getMessageProcessed();
            long tps = currentProcessed - lastProcessed.get();
            lastProcessed.set(currentProcessed);

            long duplicates = roomManager.getDuplicatesSkipped();
            long failed = roomManager.getMessageFailed();
            long receivedByServer = wsHandler.getMessagesReceived();

            System.out.printf(
                    "[METRICS] TPS: %d | Processed: %d | Duplicates: %d | " +
                            "Failed: %d | Received: %d | CPU: %.2f | Mem: %d MB%n",
                    tps, currentProcessed, duplicates,
                    failed, receivedByServer, cpuLoad, memUsed);

        } catch (Exception e) {
            System.err.println("Metrics error: " + e.getMessage());
        }
    }
}