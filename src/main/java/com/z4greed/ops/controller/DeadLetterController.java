package com.z4greed.ops.controller;

import com.z4greed.ops.dto.DeadLetterCountDto;
import com.z4greed.ops.dto.DeadLetterDetailDto;
import com.z4greed.ops.dto.DeadLetterFilterDto;
import com.z4greed.ops.dto.DeadLetterSummaryDto;
import com.z4greed.shared.core.dto.PageResponseDto;
import com.z4greed.ops.dto.ResolveDeadLetterRequestDto;
import com.z4greed.shared.core.dto.ResponseDto;
import com.z4greed.ops.service.DeadLetterService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Objects;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.web.PageableDefault;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/ops/dead-letters")
public class DeadLetterController {
    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    // Bandeja DLT. Ejemplo: GET /ops/dead-letters?status=PENDING&originalTopic=payment-processing-requests-topic&page=0&size=20
    @GetMapping
    public ResponseDto<PageResponseDto<DeadLetterSummaryDto>> search(
            DeadLetterFilterDto filter,
            @PageableDefault(size = 20, sort = "receivedAt", direction = Sort.Direction.DESC) Pageable pageable) {
        return ok("DEAD_LETTERS_FOUND", "Dead letter messages retrieved successfully", this.deadLetterService.search(filter, pageable));
    }

    @GetMapping("/stats")
    public ResponseDto<List<DeadLetterCountDto>> stats() {
        return ok("DEAD_LETTER_STATS_FOUND", "Dead letter statistics retrieved successfully", this.deadLetterService.countByTopicAndStatus());
    }

    @GetMapping("/{id}")
    public ResponseDto<DeadLetterDetailDto> get(@PathVariable Long id) {
        return ok("DEAD_LETTER_FOUND", "Dead letter message retrieved successfully", this.deadLetterService.get(id));
    }

    @PostMapping("/{id}/replay")
    public ResponseDto<DeadLetterDetailDto> replay(@AuthenticationPrincipal Jwt jwt, @PathVariable Long id, @Valid @RequestBody ResolveDeadLetterRequestDto requestDto) {
        return ok("DEAD_LETTER_REPLAYED", "Dead letter message replayed successfully", this.deadLetterService.replay(id, operatorOf(jwt), requestDto.note()));
    }

    @PostMapping("/{id}/discard")
    public ResponseDto<DeadLetterDetailDto> discard(@AuthenticationPrincipal Jwt jwt, @PathVariable Long id, @Valid @RequestBody ResolveDeadLetterRequestDto requestDto) {
        return ok("DEAD_LETTER_DISCARDED", "Dead letter message discarded successfully", this.deadLetterService.discard(id, operatorOf(jwt), requestDto.note()));
    }

    // Queda registrado quién resolvió el mensaje: su email, o su id si el token no lo trae.
    private static String operatorOf(Jwt jwt) {
        return Objects.requireNonNullElse(jwt.getClaimAsString("email"), jwt.getSubject());
    }

    private static <T> ResponseDto<T> ok(String code, String message, T data) {
        return ResponseDto.<T>builder()
                .code(code)
                .statusCode(HttpStatus.OK.value())
                .message(message)
                .data(data)
                .build();
    }
}
