package com.z4greed.ops.dto;

import com.z4greed.ops.entity.DeadLetterMessageEntity;
import com.z4greed.ops.enums.DeadLetterStatusEnum;
import java.time.LocalDateTime;

// Fila de la bandeja DLT: lo necesario para decidir qué mensaje revisar.
public record DeadLetterSummaryDto(
    Long id,
    DeadLetterStatusEnum status,
    String originalTopic,
    String eventType,
    String eventId,
    String correlationId,
    String aggregateId,
    String exceptionCauseClass,
    String exceptionMessage,
    LocalDateTime failedAt,
    LocalDateTime receivedAt) {

    public static DeadLetterSummaryDto from(DeadLetterMessageEntity entity) {
        return new DeadLetterSummaryDto(
                entity.getId(),
                entity.getStatus(),
                entity.getOriginalTopic(),
                entity.getEventType(),
                entity.getEventId(),
                entity.getCorrelationId(),
                entity.getAggregateId(),
                entity.getExceptionCauseClass(),
                entity.getExceptionMessage(),
                entity.getFailedAt(),
                entity.getReceivedAt());
    }
}
