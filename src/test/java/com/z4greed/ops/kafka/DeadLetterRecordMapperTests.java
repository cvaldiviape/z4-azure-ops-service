package com.z4greed.ops.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.z4greed.ops.entity.DeadLetterMessageEntity;
import com.z4greed.ops.enums.DeadLetterStatusEnum;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.KafkaHeaders;
import tools.jackson.databind.json.JsonMapper;

class DeadLetterRecordMapperTests {
    private final DeadLetterRecordMapper mapper = new DeadLetterRecordMapper(JsonMapper.builder().build());

    @Test
    void shouldMapDeadLetterHeadersAndEventFields() {
        String payload = """
                {"eventId":"evt-1","eventType":"PAYMENT_REQUESTED","correlationId":"purchase-1","aggregateId":"42"}
                """;
        ConsumerRecord<String, String> deadLetterRecord = new ConsumerRecord<>("payment-processing-requests-topic-dlt", 0, 7L, "42", payload);
        deadLetterRecord.headers()
                .add(KafkaHeaders.DLT_ORIGINAL_TOPIC, bytes("payment-processing-requests-topic"))
                .add(KafkaHeaders.DLT_ORIGINAL_PARTITION, ByteBuffer.allocate(Integer.BYTES).putInt(0).array())
                .add(KafkaHeaders.DLT_ORIGINAL_OFFSET, ByteBuffer.allocate(Long.BYTES).putLong(15L).array())
                .add(KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP, bytes("payment-consumer-group"))
                .add(KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN, bytes("java.lang.NullPointerException"))
                .add(KafkaHeaders.DLT_EXCEPTION_MESSAGE, bytes("boom"))
                .add(DeadLetterHeaders.REPLAY_OF_ID, bytes("3"));

        DeadLetterMessageEntity entity = this.mapper.toEntity(deadLetterRecord);

        assertThat(entity.getStatus()).isEqualTo(DeadLetterStatusEnum.PENDING);
        assertThat(entity.getDltTopic()).isEqualTo("payment-processing-requests-topic-dlt");
        assertThat(entity.getDltOffset()).isEqualTo(7L);
        assertThat(entity.getOriginalTopic()).isEqualTo("payment-processing-requests-topic");
        assertThat(entity.getOriginalPartition()).isZero();
        assertThat(entity.getOriginalOffset()).isEqualTo(15L);
        assertThat(entity.getConsumerGroup()).isEqualTo("payment-consumer-group");
        assertThat(entity.getExceptionCauseClass()).isEqualTo("java.lang.NullPointerException");
        assertThat(entity.getExceptionMessage()).isEqualTo("boom");
        assertThat(entity.getEventId()).isEqualTo("evt-1");
        assertThat(entity.getEventType()).isEqualTo("PAYMENT_REQUESTED");
        assertThat(entity.getCorrelationId()).isEqualTo("purchase-1");
        assertThat(entity.getAggregateId()).isEqualTo("42");
        assertThat(entity.getReplayOfId()).isEqualTo(3L);
        assertThat(entity.getPayload()).isEqualTo(payload);
    }

    @Test
    void shouldStoreInvalidJsonAndDeriveOriginalTopicFromName() {
        ConsumerRecord<String, String> deadLetterRecord = new ConsumerRecord<>("inventory-reservation-requests-topic-dlt", 0, 1L, null, "not-json");

        DeadLetterMessageEntity entity = this.mapper.toEntity(deadLetterRecord);

        assertThat(entity.getOriginalTopic()).isEqualTo("inventory-reservation-requests-topic");
        assertThat(entity.getPayload()).isEqualTo("not-json");
        assertThat(entity.getEventId()).isNull();
        assertThat(entity.getReplayOfId()).isNull();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
