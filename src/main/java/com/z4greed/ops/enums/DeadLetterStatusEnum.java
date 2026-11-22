package com.z4greed.ops.enums;

public enum DeadLetterStatusEnum {
    PENDING,   // Llegó al DLT y nadie lo revisó todavía.
    REPLAYED,  // Se volvió a publicar en su topic original.
    DISCARDED  // Se revisó y se decidió no reprocesarlo.
}
