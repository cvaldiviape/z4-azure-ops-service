package com.z4greed.ops.dto;

import com.z4greed.ops.enums.DeadLetterStatusEnum;

// Cantidad de mensajes por topic de origen y estado, para el resumen de la Ops Console.
public record DeadLetterCountDto(String originalTopic, DeadLetterStatusEnum status, long total) {}
