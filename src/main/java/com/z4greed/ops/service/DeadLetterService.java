package com.z4greed.ops.service;

import com.z4greed.ops.dto.DeadLetterCountDto;
import com.z4greed.ops.dto.DeadLetterDetailDto;
import com.z4greed.ops.dto.DeadLetterFilterDto;
import com.z4greed.ops.dto.DeadLetterSummaryDto;
import com.z4greed.shared.core.dto.PageResponseDto;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.domain.Pageable;

public interface DeadLetterService {
    void store(ConsumerRecord<String, String> deadLetterRecord);
    PageResponseDto<DeadLetterSummaryDto> search(DeadLetterFilterDto filter, Pageable pageable);
    DeadLetterDetailDto get(Long id);
    List<DeadLetterCountDto> countByTopicAndStatus();
    DeadLetterDetailDto replay(Long id, String resolvedBy, String note);
    DeadLetterDetailDto discard(Long id, String resolvedBy, String note);
}
