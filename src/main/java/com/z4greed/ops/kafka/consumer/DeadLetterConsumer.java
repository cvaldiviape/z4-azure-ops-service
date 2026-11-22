package com.z4greed.ops.kafka.consumer;

import com.z4greed.ops.service.DeadLetterService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DeadLetterConsumer {
    private final DeadLetterService deadLetterService;

    public DeadLetterConsumer(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    // Escucha todos los topics que terminan en -dlt, incluidos los que se creen después de arrancar.
    // Recibe el ConsumerRecord completo porque los headers traen el detalle del error.
    @KafkaListener(topicPattern = "${app.dead-letter.topic-pattern}")
    public void consume(ConsumerRecord<String, String> deadLetterRecord) {
        this.deadLetterService.store(deadLetterRecord);
    }
}
