package com.z4greed.ops.kafka.producer;

import com.z4greed.ops.entity.DeadLetterMessageEntity;
import com.z4greed.ops.enums.ErrorCodeEnum;
import com.z4greed.shared.core.exception.BusinessException;
import com.z4greed.ops.kafka.DeadLetterHeaders;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

// Vuelve a publicar un mensaje del DLT en su topic original, con el mismo key y payload.
// No copia los headers del DLT: el servicio consumidor lo recibe como un mensaje nuevo.
@Component
public class DeadLetterReplayPublisher {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final long replayTimeoutMilliseconds;

    public DeadLetterReplayPublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.dead-letter.replay-timeout-milliseconds}") long replayTimeoutMilliseconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.replayTimeoutMilliseconds = replayTimeoutMilliseconds;
    }

    public void publish(DeadLetterMessageEntity deadLetterMessage) {
        ProducerRecord<String, String> replayRecord = new ProducerRecord<>(
                deadLetterMessage.getOriginalTopic(),
                deadLetterMessage.getMessageKey(),
                deadLetterMessage.getPayload());
        replayRecord.headers().add(DeadLetterHeaders.REPLAY_OF_ID, deadLetterMessage.getId().toString().getBytes(StandardCharsets.UTF_8));

        try {
            this.kafkaTemplate.send(replayRecord).get(this.replayTimeoutMilliseconds, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new BusinessException(ErrorCodeEnum.REPLAY_FAILED);
        } catch (Exception exception) {
            throw new BusinessException(ErrorCodeEnum.REPLAY_FAILED);
        }
    }
}
