package com.z4greed.ops.repository;

import com.z4greed.ops.dto.DeadLetterFilterDto;
import com.z4greed.ops.entity.DeadLetterMessageEntity;
import org.springframework.data.jpa.domain.Specification;

// Traduce los filtros opcionales de la bandeja DLT a condiciones SQL. Un filtro vacío no restringe nada.
public final class DeadLetterMessageSpecifications {

    private DeadLetterMessageSpecifications() {
    }

    public static Specification<DeadLetterMessageEntity> matching(DeadLetterFilterDto filter) {
        return Specification.<DeadLetterMessageEntity>unrestricted()
                .and(equalTo("status", filter.status()))
                .and(equalTo("originalTopic", filter.originalTopic()))
                .and(equalTo("eventType", filter.eventType()))
                .and(equalTo("correlationId", filter.correlationId()))
                .and(equalTo("aggregateId", filter.aggregateId()));
    }

    private static Specification<DeadLetterMessageEntity> equalTo(String attribute, Object value) {
        if (value == null || (value instanceof String text && text.isBlank())) {
            return Specification.unrestricted();
        }
        return (root, query, criteriaBuilder) -> criteriaBuilder.equal(root.get(attribute), value);
    }
}
