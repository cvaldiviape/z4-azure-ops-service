package com.z4greed.ops.entity;

import com.z4greed.ops.enums.DeadLetterStatusEnum;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import lombok.*;

@Entity
@Table(name = "dead_letter_messages")
@Getter
@Setter
@Builder
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class DeadLetterMessageEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String dltTopic;
    private Integer dltPartition;
    private Long dltOffset;

    private String originalTopic;
    private Integer originalPartition;
    private Long originalOffset;
    private String consumerGroup;

    private String messageKey;
    private String payload;

    private String eventId;
    private String eventType;
    private String correlationId;
    private String aggregateId;

    private String exceptionClass;
    private String exceptionCauseClass;
    private String exceptionMessage;
    private String exceptionStacktrace;

    @Enumerated(EnumType.STRING)
    private DeadLetterStatusEnum status;

    private LocalDateTime failedAt;
    private LocalDateTime receivedAt;
    private Long replayOfId;

    private String resolvedBy;
    private LocalDateTime resolvedAt;
    private String resolutionNote;

    public boolean isPending() {
        return this.status == DeadLetterStatusEnum.PENDING;
    }

    public void resolve(DeadLetterStatusEnum resolution, String resolvedBy, String note) {
        this.status = resolution;
        this.resolvedBy = resolvedBy;
        this.resolutionNote = note;
        this.resolvedAt = LocalDateTime.now();
    }
}
