package com.z4greed.ops.repository;

import com.z4greed.ops.dto.DeadLetterCountDto;
import com.z4greed.ops.entity.DeadLetterMessageEntity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;

public interface DeadLetterMessageRepository extends JpaRepository<DeadLetterMessageEntity, Long>, JpaSpecificationExecutor<DeadLetterMessageEntity> {
    boolean existsByDltTopicAndDltPartitionAndDltOffset(String dltTopic, Integer dltPartition, Long dltOffset);

    @Query("""
            SELECT new com.z4greed.ops.dto.DeadLetterCountDto(d.originalTopic, d.status, COUNT(d))
            FROM DeadLetterMessageEntity d
            GROUP BY d.originalTopic, d.status
            ORDER BY d.originalTopic, d.status
            """)
    List<DeadLetterCountDto> countByTopicAndStatus();
}
