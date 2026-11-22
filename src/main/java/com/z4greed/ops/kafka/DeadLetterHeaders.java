package com.z4greed.ops.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

// Lee los headers que DeadLetterPublishingRecoverer agrega al mensaje cuando lo envía al DLT.
// Los textos vienen en UTF-8; la partición es un int de 4 bytes y el offset un long de 8 bytes.
public final class DeadLetterHeaders {
    // Header propio: lo agrega ops-service al reprocesar, para enlazar un nuevo fallo con el mensaje original.
    public static final String REPLAY_OF_ID = "z4-replay-of-dead-letter-id";

    private DeadLetterHeaders() {
    }

    public static String text(Headers headers, String name) {
        byte[] value = value(headers, name);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    public static Integer integer(Headers headers, String name) {
        byte[] value = value(headers, name);
        return value == null || value.length != Integer.BYTES ? null : ByteBuffer.wrap(value).getInt();
    }

    public static Long number(Headers headers, String name) {
        byte[] value = value(headers, name);
        return value == null || value.length != Long.BYTES ? null : ByteBuffer.wrap(value).getLong();
    }

    private static byte[] value(Headers headers, String name) {
        Header header = headers.lastHeader(name);
        return header == null ? null : header.value();
    }
}
