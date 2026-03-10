package com.chatflow.server.mq;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;



public class ConsumerWorker implements Runnable {
    private final List<String> roomIds;
    private final KafkaProducerPool kafkaProducerPool;
    private final RoomManager roomManager;
    private volatile boolean running = true;


    private static final String BOOTSTRAP_SERVERS =
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    public ConsumerWorker(List<String> roomIds, KafkaProducerPool kafkaProducerPool, RoomManager roomManager) {
        this.roomIds = roomIds;
        this.kafkaProducerPool = kafkaProducerPool;
        this.roomManager = roomManager;
    }

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "chat-consumer-group-" + Thread.currentThread().getId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // 手动commit
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100); // 相当于 prefetch

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(roomIds);

        while (running) {
            ConsumerRecords<String,String> records =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record: records) {
                boolean success = false;
                int retries = 3;

                while (retries > 0 && !success) {
                    try {
                        roomManager.broadcast(record.topic(), record.value());
                        success = true;
                    } catch (Exception e) {
                        retries--;
                        if (retries == 0) {
                            kafkaProducerPool.sendToDLQ(
                                    new ProducerRecord<>("dlq.chat", record.key(), record.value()), e);
                        } else {
                            try {Thread.sleep(100);}
                            catch (InterruptedException ignored) {}
                        }
                    }
                }

                consumer.commitSync(Collections.singletonMap(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1)
                ));
            }


        }
        consumer.close();

    }

//for i in $(seq 1 20); do
//    docker exec kafka /opt/kafka/bin/kafka-topics.sh \
//            --create \
//            --bootstrap-server localhost:9092 \
//            --topic room.$i \
//            --partitions 3 \
//            --replication-factor 1
//    done
//
//# 创建 DLQ topic
//    docker exec kafka /opt/kafka/bin/kafka-topics.sh \
//            --create \
//            --bootstrap-server localhost:9092 \
//            --topic dlq.chat \
//            --partitions 1 \
//            --replication-factor 1
}
