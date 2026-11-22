package com.z4greed.ops.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.z4greed.ops.dto.DeadLetterDetailDto;
import com.z4greed.ops.entity.DeadLetterMessageEntity;
import com.z4greed.ops.enums.DeadLetterStatusEnum;
import com.z4greed.ops.enums.ErrorCodeEnum;
import com.z4greed.shared.core.exception.BusinessException;
import com.z4greed.ops.kafka.DeadLetterRecordMapper;
import com.z4greed.ops.kafka.producer.DeadLetterReplayPublisher;
import com.z4greed.ops.repository.DeadLetterMessageRepository;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DeadLetterServiceImplTests {
    @Mock private DeadLetterMessageRepository repository;
    @Mock private DeadLetterRecordMapper recordMapper;
    @Mock private DeadLetterReplayPublisher replayPublisher;
    @InjectMocks private DeadLetterServiceImpl service;

    @Test
    void shouldNotStoreTheSameDeadLetterTwice() {
        ConsumerRecord<String, String> deadLetterRecord = new ConsumerRecord<>("orders-dlt", 0, 5L, "1", "{}");
        when(this.repository.existsByDltTopicAndDltPartitionAndDltOffset("orders-dlt", 0, 5L)).thenReturn(true);

        this.service.store(deadLetterRecord);

        verify(this.repository, never()).save(any());
    }

    @Test
    void shouldMarkAsReplayedOnlyAfterPublishing() {
        DeadLetterMessageEntity pending = pending();
        when(this.repository.findById(1L)).thenReturn(Optional.of(pending));

        DeadLetterDetailDto result = this.service.replay(1L, "admin@z4greed.com", "Database was down");

        verify(this.replayPublisher).publish(pending);
        assertThat(result.status()).isEqualTo(DeadLetterStatusEnum.REPLAYED);
        assertThat(result.resolvedBy()).isEqualTo("admin@z4greed.com");
        assertThat(result.resolutionNote()).isEqualTo("Database was down");
        assertThat(result.resolvedAt()).isNotNull();
    }

    @Test
    void shouldKeepPendingWhenReplayFails() {
        DeadLetterMessageEntity pending = pending();
        when(this.repository.findById(1L)).thenReturn(Optional.of(pending));
        doThrow(new BusinessException(ErrorCodeEnum.REPLAY_FAILED)).when(this.replayPublisher).publish(pending);

        assertThatThrownBy(() -> this.service.replay(1L, "admin", "retry"))
                .isInstanceOf(BusinessException.class);
        assertThat(pending.getStatus()).isEqualTo(DeadLetterStatusEnum.PENDING);
    }

    @Test
    void shouldRejectResolvingAnAlreadyResolvedMessage() {
        DeadLetterMessageEntity discarded = pending();
        discarded.resolve(DeadLetterStatusEnum.DISCARDED, "admin", "duplicate");
        when(this.repository.findById(1L)).thenReturn(Optional.of(discarded));

        assertThatThrownBy(() -> this.service.replay(1L, "admin", "again"))
                .isInstanceOfSatisfying(BusinessException.class,
                        exception -> assertThat(exception.getErrorCode()).isEqualTo(ErrorCodeEnum.DEAD_LETTER_ALREADY_RESOLVED));
        verify(this.replayPublisher, never()).publish(any());
    }

    private static DeadLetterMessageEntity pending() {
        return DeadLetterMessageEntity.builder()
                .id(1L)
                .originalTopic("payment-processing-requests-topic")
                .messageKey("42")
                .payload("{}")
                .status(DeadLetterStatusEnum.PENDING)
                .build();
    }
}
