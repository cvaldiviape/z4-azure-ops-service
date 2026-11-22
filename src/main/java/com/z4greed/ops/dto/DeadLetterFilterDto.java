package com.z4greed.ops.dto;

import com.z4greed.ops.enums.DeadLetterStatusEnum;

// Filtros opcionales de la bandeja DLT. aggregateId es el id del pedido.
public record DeadLetterFilterDto(
    DeadLetterStatusEnum status,
    String originalTopic,
    String eventType,
    String correlationId,
    String aggregateId) {}
