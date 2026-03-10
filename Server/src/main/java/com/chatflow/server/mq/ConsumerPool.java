package com.chatflow.server.mq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerPool {

    private final int THREAD_COUNT = 40;  // 20 threads (1 per room)
    private final int ROOM_COUNT = 20;    // 20 rooms
    private final ExecutorService executor;
    private final KafkaProducerPool kafkaProducerPool;
    private final RoomManager roomManager;

    public ConsumerPool(KafkaProducerPool kafkaProducerPool, RoomManager roomManager) {
        this.kafkaProducerPool = kafkaProducerPool;
        this.roomManager = roomManager;
        this.executor = Executors.newFixedThreadPool(THREAD_COUNT);
    }

    public void init(){
        // 把 20 个房间分给 20 个线程
        List<List<String>> roomGroups = distributeRooms();

        for (List<String> rooms : roomGroups) {
            try {
                executor.submit(new ConsumerWorker(rooms, kafkaProducerPool, roomManager));
            } catch (Exception e) {
                System.err.println("Failed to create consumer: " + e.getMessage());
            }
        }
        System.out.println("ConsumerPool started with " + THREAD_COUNT + " threads handling " + ROOM_COUNT + " rooms");
    }

    // 每个 room 分配给 4 个线程
    private List<List<String>> distributeRooms() {
        List<List<String>> roomGroups = new ArrayList<>();
        // 1. 初始化对应数量的线程组
        for (int i = 0; i < THREAD_COUNT; i++) {
            roomGroups.add(new ArrayList<>());
        }

        // 2. 使用取模运算，把房间均匀撒到现有的线程里
        // 这样无论 ROOM_COUNT 是 20 还是 200，无论 THREAD_COUNT 是 2 还是 20，都不会越界
        for (int room = 1; room <= ROOM_COUNT; room++) {
            int threadIndex = (room - 1) % THREAD_COUNT; // 关键改动！
            roomGroups.get(threadIndex).add("room." + room);
        }

        return roomGroups;

    }
}
