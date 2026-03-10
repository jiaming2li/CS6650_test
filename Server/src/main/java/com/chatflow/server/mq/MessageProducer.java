package com.chatflow.server.mq;

import com.chatflow.server.model.QueueMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MessageProducer {

    private final KafkaProducerPool producerPool;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public MessageProducer(KafkaProducerPool producerPool) {
        this.producerPool = producerPool;
    }

    public void publish(QueueMessage message) throws Exception {
        String topic = "room." + message.getRoomId();
        String value = objectMapper.writeValueAsString(message);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, message.getMessageId(), value);

        KafkaProducer<String, String> producer = null;
        try {
            producer = producerPool.borrowProducer();
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    producerPool.sendToDLQ(record, exception);
                    producerPool.recordFailure();
                }
            });
        } finally {
            if (producer != null) producerPool.returnProducer(producer);
        }
    }
}