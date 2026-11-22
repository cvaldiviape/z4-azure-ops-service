package com.z4greed.ops.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

// Motivo de la resolución. Es obligatorio para que quede claro por qué se reprocesó o descartó un mensaje.
public record ResolveDeadLetterRequestDto(
    @NotBlank @Size(max = 1000) String note) {}
