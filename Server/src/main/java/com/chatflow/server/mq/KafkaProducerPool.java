package com.chatflow.server.mq;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.atomic.*;


public class KafkaProducerPool {
    private final KafkaProducer<String,String> producer;
    private final KafkaProducer<String, String> dlqProducer;

    //熔断器
    private final AtomicBoolean circuitOpen = new AtomicBoolean(false);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private volatile long circuitOpenTime = 0;
    private static final int FAILURE_THRESHOLD = 500; // 调大到 500
    private static final long RESET_MS = 5000;        // 缩短到 5 秒

    //mq parameters - use same as RabbitMQConfig
    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    public KafkaProducerPool(int poolSize) {
        this.producer = createProducer();
        this.dlqProducer = createProducer();
    }

    public KafkaProducer<String,String> createProducer() { //producer对应connection
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);//64kb
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);//10ms
        props.put(ProducerConfig.RETRIES_CONFIG, 3);//retry
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        return new KafkaProducer<>(props);
    }

    public KafkaProducer<String, String> borrowProducer() throws Exception {
        //熔断器检查
        //熔断器的三种状态，open/close/semi-open
        if (circuitOpen.get()) {
            long elapsed = System.currentTimeMillis() - circuitOpenTime;
            if (elapsed < RESET_MS) {
                throw new InterruptedException("Circuit breaker open");
            }
            circuitOpen.set(false);
            failureCount.set(0);
        }

       return producer;
    }

    public void returnProducer(KafkaProducer<String, String> producer) {
        //
    }


    public void shutdown() {
        producer.close();
        dlqProducer.close();
    }


    public void sendToDLQ(ProducerRecord<String, String> record, Exception e) {
        try {
            dlqProducer.send(new ProducerRecord<>("dlq.chat", record.key(), record.value()));
            System.err.println("Sent to DLQ: " + e.getMessage());
        } catch (Exception ex) {
            System.err.println("Failed to send to DLQ: " + ex.getMessage());
        }
    }



    public void recordFailure() {
        int count = failureCount.incrementAndGet();
        if (count >= FAILURE_THRESHOLD) {
            tripCircuit();  // 累计失败500次才触发熔断
        }
    }


    private void tripCircuit() {
        if (circuitOpen.compareAndSet(false, true)) {
            circuitOpenTime = System.currentTimeMillis();
            failureCount.set(0);  // 重置计数
            System.err.println("Circuit breaker TRIPPED after " + FAILURE_THRESHOLD + " failures");
        }
    }


}
