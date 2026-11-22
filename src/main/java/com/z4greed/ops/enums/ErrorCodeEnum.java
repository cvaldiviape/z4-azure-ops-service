package com.z4greed.ops.enums;

import com.z4greed.shared.core.exception.ErrorCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;

// VALIDATION_ERROR ya no está aquí: lo aporta CoreErrorCode de z4-shared-core.
@Getter
@RequiredArgsConstructor
public enum ErrorCodeEnum implements ErrorCode {
    DEAD_LETTER_NOT_FOUND(HttpStatus.NOT_FOUND, "Dead letter message not found"),
    DEAD_LETTER_ALREADY_RESOLVED(HttpStatus.CONFLICT, "Dead letter message was already resolved"),
    REPLAY_FAILED(HttpStatus.BAD_GATEWAY, "Kafka did not confirm the replayed message");
    private final HttpStatus httpStatus;
    private final String message;
}
