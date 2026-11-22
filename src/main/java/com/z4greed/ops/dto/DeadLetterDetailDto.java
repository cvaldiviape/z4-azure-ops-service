package com.z4greed.ops.dto;

import com.z4greed.ops.entity.DeadLetterMessageEntity;
import com.z4greed.ops.enums.DeadLetterStatusEnum;
import java.time.LocalDateTime;

// Detalle completo de un mensaje del DLT: payload, error y resolución.
public record DeadLetterDetailDto(
    Long id,
    DeadLetterStatusEnum status,
    String dltTopic,
    Integer dltPartition,
    Long dltOffset,
    String originalTopic,
    Integer originalPartition,
    Long originalOffset,
    String consumerGroup,
    String messageKey,
    String payload,
    String eventId,
    String eventType,
    String correlationId,
    String aggregateId,
    String exceptionClass,
    String exceptionCauseClass,
    String exceptionMessage,
    String exceptionStacktrace,
    LocalDateTime failedAt,
    LocalDateTime receivedAt,
    Long replayOfId,
    String resolvedBy,
    LocalDateTime resolvedAt,
    String resolutionNote) {

    public static DeadLetterDetailDto from(DeadLetterMessageEntity entity) {
        return new DeadLetterDetailDto(
                entity.getId(),
                entity.getStatus(),
                entity.getDltTopic(),
                entity.getDltPartition(),
                entity.getDltOffset(),
                entity.getOriginalTopic(),
                entity.getOriginalPartition(),
                entity.getOriginalOffset(),
                entity.getConsumerGroup(),
                entity.getMessageKey(),
                entity.getPayload(),
                entity.getEventId(),
                entity.getEventType(),
                entity.getCorrelationId(),
                entity.getAggregateId(),
                entity.getExceptionClass(),
                entity.getExceptionCauseClass(),
                entity.getExceptionMessage(),
                entity.getExceptionStacktrace(),
                entity.getFailedAt(),
                entity.getReceivedAt(),
                entity.getReplayOfId(),
                entity.getResolvedBy(),
                entity.getResolvedAt(),
                entity.getResolutionNote());
    }
}
