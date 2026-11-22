package com.z4greed.ops.kafka;

import com.z4greed.ops.entity.DeadLetterMessageEntity;
import com.z4greed.ops.enums.DeadLetterStatusEnum;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

// Convierte un mensaje leído de un DLT en una fila de dead_letter_messages.
@Component
public class DeadLetterRecordMapper {
    private static final String DLT_SUFFIX = "-dlt";
    private static final int MAX_SHORT_TEXT = 255;
    private static final int MAX_CLASS_NAME = 500;

    private final ObjectMapper mapper;

    public DeadLetterRecordMapper(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public DeadLetterMessageEntity toEntity(ConsumerRecord<String, String> deadLetterRecord) {
        Headers headers = deadLetterRecord.headers();
        JsonNode event = this.parseEvent(deadLetterRecord.value());

        return DeadLetterMessageEntity.builder()
                .dltTopic(deadLetterRecord.topic())
                .dltPartition(deadLetterRecord.partition())
                .dltOffset(deadLetterRecord.offset())
                .originalTopic(this.resolveOriginalTopic(deadLetterRecord))
                .originalPartition(DeadLetterHeaders.integer(headers, KafkaHeaders.DLT_ORIGINAL_PARTITION))
                .originalOffset(DeadLetterHeaders.number(headers, KafkaHeaders.DLT_ORIGINAL_OFFSET))
                .consumerGroup(truncate(DeadLetterHeaders.text(headers, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP), MAX_SHORT_TEXT))
                .messageKey(truncate(deadLetterRecord.key(), MAX_SHORT_TEXT))
                .payload(deadLetterRecord.value())
                .eventId(text(event, "eventId"))
                .eventType(text(event, "eventType"))
                .correlationId(text(event, "correlationId"))
                .aggregateId(text(event, "aggregateId"))
                .exceptionClass(truncate(DeadLetterHeaders.text(headers, KafkaHeaders.DLT_EXCEPTION_FQCN), MAX_CLASS_NAME))
                .exceptionCauseClass(truncate(DeadLetterHeaders.text(headers, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN), MAX_CLASS_NAME))
                .exceptionMessage(DeadLetterHeaders.text(headers, KafkaHeaders.DLT_EXCEPTION_MESSAGE))
                .exceptionStacktrace(DeadLetterHeaders.text(headers, KafkaHeaders.DLT_EXCEPTION_STACKTRACE))
                .status(DeadLetterStatusEnum.PENDING)
                .failedAt(toLocalDateTime(deadLetterRecord.timestamp()))
                .receivedAt(LocalDateTime.now())
                .replayOfId(this.parseReplayOfId(headers))
                .build();
    }

    private String resolveOriginalTopic(ConsumerRecord<String, String> deadLetterRecord) {
        String originalTopic = DeadLetterHeaders.text(deadLetterRecord.headers(), KafkaHeaders.DLT_ORIGINAL_TOPIC);
        if (originalTopic != null) {
            return originalTopic;
        }
        String deadLetterTopic = deadLetterRecord.topic();
        return deadLetterTopic.endsWith(DLT_SUFFIX)
                ? deadLetterTopic.substring(0, deadLetterTopic.length() - DLT_SUFFIX.length())
                : deadLetterTopic;
    }

    // Un payload que no es JSON también se guarda: justamente puede ser el motivo por el que llegó al DLT.
    private JsonNode parseEvent(String payload) {
        if (payload == null) {
            return null;
        }
        try {
            return this.mapper.readTree(payload);
        } catch (Exception exception) {
            return null;
        }
    }

    private Long parseReplayOfId(Headers headers) {
        String replayOfId = DeadLetterHeaders.text(headers, DeadLetterHeaders.REPLAY_OF_ID);
        try {
            return replayOfId == null ? null : Long.valueOf(replayOfId);
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private static String text(JsonNode event, String fieldName) {
        if (event == null || !event.hasNonNull(fieldName)) {
            return null;
        }
        return truncate(event.get(fieldName).asText(), 100);
    }

    private static String truncate(String value, int maxLength) {
        return value == null || value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    private static LocalDateTime toLocalDateTime(long epochMilliseconds) {
        return epochMilliseconds <= 0 ? null : LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMilliseconds), ZoneId.systemDefault());
    }
}
