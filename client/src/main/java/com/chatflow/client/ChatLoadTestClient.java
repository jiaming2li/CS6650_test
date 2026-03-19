package com.chatflow.client;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import com.chatflow.client.model.ChatMessage;

/**
 * Basic Chat Load Test Client
 * 
 * Required Metrics (Assignment 1):
 * 1. Number of successful messages sent
 * 2. Number of failed messages
 * 3. Total runtime (wall time)
 * 4. Overall throughput (messages/second)
 * 5. Connection statistics (total connections, reconnections)
 */
public class ChatLoadTestClient {
    
    // Configuration
    private static final int TOTAL_MESSAGES = 500_000;
    private static final int TOTAL_ROOMS = 20;
    private static final int CONNECTIONS_PER_ROOM = 5;
    private static final int MAIN_THREADS = 32;
    private static final String SERVER_HOST = System.getenv("SERVER_HOST") != null ? 
        System.getenv("SERVER_HOST") : "localhost";
    private static final int SERVER_PORT = 8080;
    //private static final String SERVER_URL = "ws://" + SERVER_HOST + ":" + SERVER_PORT + "/chat/room/";
    //private static final String SERVER_URL = "ws://CS6650a2alb-1305823827.us-east-1.elb.amazonaws.com/chat/room/";
    private static final String SERVER_URL = "ws://54.242.238.45:8080/chat/room/";

    // Message queues
    private final BlockingQueue<ChatMessage> warmupQueue = new LinkedBlockingQueue<>(32000);
    private final BlockingQueue<ChatMessage> mainQueue = new LinkedBlockingQueue<>(500000);

    // Metrics (Assignment 1 Requirements)
    private final AtomicInteger sentCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger totalConnections = new AtomicInteger(0);
    private final AtomicInteger reconnectionCount = new AtomicInteger(0);

    // Latency tracking - stores all latency samples
    private final List<Long> latencies = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger latencySampleCounter = new AtomicInteger(0);
    private static final int LATENCY_SAMPLE_RATE = 100; // Sample 1 out of every 100 messages

    // Connection pools
    private final ConcurrentHashMap<Integer, List<WebSocketClient>> roomPools = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, AtomicInteger> roomCounters = new ConcurrentHashMap<>();

    // Rate limiting: 限制每秒发送的消息数
    private static final int MAX_SEND_PER_SECOND = 2000; // Increased to stress test server
    private static final AtomicInteger lastSecondSent = new AtomicInteger(0);
    private static volatile long lastSecondResetTime = System.currentTimeMillis();

    // Retry configuration
    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_BACKOFF_MS = 1;
    private static final long MAX_BACKOFF_MS = 50;

    // Track main test duration
    private long mainTestStart = 0;
    private long mainTestEnd = 0;

    // Message pool
    private static final String[] MESSAGES = {
        "Hello!", "Hi?", "Hi there", "Morning",
        "Thx", "See you", "OK", "Great!", "Nice"
    };
    
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        new ChatLoadTestClient().run();
        long endTime = System.currentTimeMillis();
        System.out.println("\n" + "=".repeat(50));
        System.out.println("Total runtime: " + (endTime - startTime) / 1000.0 + "s");
    }

    public void run() throws Exception {
        System.out.println("=== ChatLoadTestClient (Basic) ===");

        // Main test
        runMainTest();

        // Print results
        printResults();
    }
    


    
    private void runMainTest() throws Exception {
        System.out.println("\n=== Main Phase ===");
        
        // Initialize connection pools
        System.out.println("Initializing " + (TOTAL_ROOMS * CONNECTIONS_PER_ROOM) + " connections...");
        for (int roomId = 1; roomId <= TOTAL_ROOMS; roomId++) {
            roomPools.put(roomId, new CopyOnWriteArrayList<>());
            roomCounters.put(roomId, new AtomicInteger(0));
            for (int c = 0; c < CONNECTIONS_PER_ROOM; c++) {
                WebSocketClient client = createConnection(roomId);
                roomPools.get(roomId).add(client);
                totalConnections.incrementAndGet();
                Thread.sleep(10);
            }
            // Only log rooms 1-3 and last room
            if (roomId <= 3 || roomId == TOTAL_ROOMS) {
                System.out.println("Room " + roomId + ": " + CONNECTIONS_PER_ROOM + " connections established");
            }
        }
        System.out.println("...");
        System.out.println("All connections established!");
        
        // Start message generator in background
        Thread generator = startMainMessageGenerator();

        // Start sender threads
        ExecutorService executor = Executors.newFixedThreadPool(MAIN_THREADS);
        CountDownLatch latch = new CountDownLatch(MAIN_THREADS);
        long testStart = System.currentTimeMillis();
        mainTestStart = testStart;
        
        for (int i = 0; i < MAIN_THREADS; i++) {
            executor.submit(() -> {
                try {
                    while (true) {
                        ChatMessage msg = mainQueue.poll(1, TimeUnit.SECONDS);
                        if (msg != null) {
                            sendToRoom(msg);
                        }
                        // Only exit when queue is empty AND generator is done
                        if (mainQueue.isEmpty() && !generator.isAlive()) {
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }
        
        latch.await();
        executor.shutdown();
        generator.join();
        
        // Wait for server to process all messages
        while (successCount.get() < TOTAL_MESSAGES) {
            Thread.sleep(100);
        }

        long testEnd = System.currentTimeMillis();
        
        Thread.sleep(10000);

        mainTestEnd = testEnd;
        System.out.println("Main phase completed in " + (testEnd - testStart) / 1000.0 + "s");
    }

    private Thread startMainMessageGenerator() {
        Thread generator = new Thread(() -> {
            System.out.println("Generating main messages...");
            for (int i = 0; i < TOTAL_MESSAGES; i++) {
                try {
                    mainQueue.put(createRandomMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            System.out.println("Message generation complete!");
        });
        generator.start();
        return generator;
    }

    private void sendToRoom(ChatMessage msg) {
        // Rate limiting: 如果这秒已经发太多了，等一等
        long now = System.currentTimeMillis();
        if (now - lastSecondResetTime > 1000) {
            lastSecondResetTime = now;
            lastSecondSent.set(0);
        }

        if (lastSecondSent.incrementAndGet() > MAX_SEND_PER_SECOND) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException ignored) {}
        }

        int roomId = msg.getRoomId();
        List<WebSocketClient> pool = roomPools.get(roomId);
        if (pool == null || pool.isEmpty()) {
            failureCount.incrementAndGet();
            return;
        }

        int index = roomCounters.get(roomId).getAndIncrement() % pool.size();
        WebSocketClient client = pool.get(index);

        // Set send time for latency tracking
        msg.setSendTime(System.currentTimeMillis());

        if (client != null && client.isOpen()) {
            sendWithRetry(client, msg);
        } else {
            // Connection dead, mark as failure and try to reconnect in background?
            // For now just fail
            failureCount.incrementAndGet();
        }
    }
    
    //private void sendWithRetry(WebSocketClient client, ChatMessage msg) {
    //    try {
    //        client.send(toJson(msg));
    //        sentCount.getAndIncrement();
            // System.out.println("Sent message: " + msg.getMessageId());
    //        } catch (Exception e) {
    //        reconnectionCount.incrementAndGet();
    //        failureCount.incrementAndGet();
            // System.err.println("Send failed: " + e.getMessage());
    //    }
    //}

    private void sendWithRetry(WebSocketClient client, ChatMessage msg) {

        String json = toJson(msg);

        int attempt = 0;
        long backoffMs = INITIAL_BACKOFF_MS;

        while (attempt < MAX_RETRIES) {
            if (client != null && client.isOpen()) {
                try {
                    client.send(json);
                    sentCount.incrementAndGet();
                    return;
                } catch (Exception e) {
                    // Send failed, will retry
                }
            }

            // Exponential backoff with cap
            attempt++;
            if (attempt < MAX_RETRIES) {
                try {
                    Thread.sleep(backoffMs);
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);  // Cap at MAX_BACKOFF_MS

                    // Try to reconnect
                    client = handleReconnection(client, msg.getRoomId());

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        failureCount.incrementAndGet();
    }


    private WebSocketClient handleReconnection(WebSocketClient oldClient, int roomId) {
        System.err.println("RECONNECTING room " + roomId +
                " attempt at processed=" + successCount.get());
        reconnectionCount.incrementAndGet();

        try {
            if (oldClient != null) {
                try {
                    oldClient.closeBlocking();
                } catch (Exception e) {
                    // Ignore close errors
                }
            }

            WebSocketClient newClient = createConnection(roomId);

            if (newClient.isOpen()) {
                // Update connection pool
                List<WebSocketClient> connections = roomPools.get(roomId);
                if (connections != null) {
                    synchronized (connections) {
                        connections.remove(oldClient);
                        connections.add(newClient);
                    }
                }
                return newClient;
            }
        } catch (Exception e) {
            System.err.println("Reconnection failed: " + e.getMessage());
        }
        return null;
    }

    private void printResults() {

        System.out.println("=".repeat(50));
        System.out.println("CLIENT METRICS");
        System.out.println("=".repeat(50));
        
        long sent = sentCount.get();
        long received = successCount.get();
        long failed = failureCount.get();
        long connections = totalConnections.get();
        long reconnections = reconnectionCount.get();
        
        // Calculate duration
        double actualDuration = (mainTestEnd - mainTestStart) / 1000.0;
        double throughput = sent / actualDuration;

        System.out.printf("Runtime: %.2f seconds%n", actualDuration);
        System.out.printf("Total Sent: %d%n", sent);
        System.out.printf("Total Received (SUCCESS): %d%n", received);
        System.out.printf("Send Errors (Failed): %d%n", failed);
        System.out.printf("Active Connections: %d%n", connections);
        System.out.printf("Reconnections: %d%n", reconnections);
        System.out.printf("Throughput (TPS): %.2f msg/s%n", throughput);
        
        System.out.println("=".repeat(50));
        
        // Calculate message loss percentage
        if (sent > 0) {
            double lossPercent = ((double) failureCount.get() / TOTAL_MESSAGES) * 100;
            System.out.printf("Send Failures: %.2f%%%n", lossPercent);
        }
        
        // Latency statistics
        if (!latencies.isEmpty()) {
            List<Long> snapshot = new ArrayList<>(latencies);
            Collections.sort(snapshot);
            
            // mean
            long sum = 0;
            for (long v : snapshot) sum += v;
            double mean = sum / (double) snapshot.size();
            
            // p95, p99
            long p95 = percentile(snapshot, 95.0);
            long p99 = percentile(snapshot, 99.0);
            
            System.out.println("Latency Samples: " + snapshot.size());
            System.out.printf("Latency Mean: %.2f ms%n", mean);
            System.out.printf("Latency p95: %d ms%n", p95);
            System.out.printf("Latency p99: %d ms%n", p99);
        }
        
        System.out.println("=".repeat(50));
    }
    
    private long percentile(List<Long> sorted, double p) {
        if (sorted.isEmpty()) return 0;
        double rank = (p / 100.0) * (sorted.size() - 1);
        int idx = (int) Math.round(rank);
        if (idx < 0) idx = 0;
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        return sorted.get(idx);
    }

    // ============ Helper Methods ============

    private ChatMessage createRandomMessage() {
        ChatMessage msg = new ChatMessage();
        msg.setUserId(String.valueOf(new Random().nextInt(100000) + 1)); // 数字 1-100000
        msg.setUsername("User" + new Random().nextInt(100000));
        msg.setMessage(MESSAGES[new Random().nextInt(MESSAGES.length)]);
        msg.setRoomId(new Random().nextInt(TOTAL_ROOMS) + 1);
        msg.setMessageType("TEXT");
        msg.setTimestamp(Instant.now().toString());
        msg.setNonce(UUID.randomUUID().toString());
        return msg;
    }


    private WebSocketClient createConnection(int roomId) throws Exception {
        URI uri = new URI(SERVER_URL + roomId);
        WebSocketClient client = new WebSocketClient(uri) {
            @Override
            public void onOpen(ServerHandshake h) {
                // Note: To set TCP buffer size (SO_SNDBUF/SO_RCVBUF), run with:
                // -Dexec.jvmargs="-Xms512m -Xmx4g" (Maven)
                // or System.setProperty("java.net.preferIPv4Stack", "true");
            }
            @Override
            public void onMessage(String m) {
                //System.out.println("Received from server: " + m);
                if (m == null || m.isEmpty()) return;

                // 统计所有收到的消息，不管是直接回复还是广播
                successCount.incrementAndGet();

                // latency只从SUCCESS回复里算（直接回复才有sendTime）
                if (m.contains("SUCCESS")) {
                    if (latencySampleCounter.incrementAndGet() % LATENCY_SAMPLE_RATE == 0) {
                        try {
                            int idx = m.indexOf("\"sendTime\":");
                            if (idx > 0) {
                                int start = idx + "\"sendTime\":".length();
                                int end = m.indexOf(',', start);
                                if (end < 0) end = m.indexOf('}', start);
                                long sendTime = Long.parseLong(m.substring(start, end).trim());
                                long latency = System.currentTimeMillis() - sendTime;
                                latencies.add(latency);
                            }
                        } catch (Exception ignored) {}
                    }
                }
            }
            @Override
            public void onClose(int c, String r, boolean remote) {
                // System.out.println("Connection closed: " + c + " " + r);
            }
            @Override
            public void onError(Exception e) {
                // System.err.println("WebSocket error: " + e.getMessage());
            }
        };
        client.connectBlocking();
        return client;
    }
    
    private String toJson(ChatMessage msg) {
        return String.format(
            "{\"userId\":\"%s\",\"username\":\"%s\",\"message\":\"%s\",\"timestamp\":\"%s\",\"messageType\":\"%s\",\"roomId\":%d,\"nonce\":\"%s\"}",
            msg.getUserId(), msg.getUsername(), msg.getMessage(), msg.getTimestamp(),
            msg.getMessageType(), msg.getRoomId(), msg.getNonce()
        );
    }
}
