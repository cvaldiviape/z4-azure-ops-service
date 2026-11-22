package com.z4greed.ops.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    // Si no se puede guardar un mensaje del DLT (por ejemplo, la BD está caída), se reintenta sin límite.
    // Así nunca se confirma un offset de algo que no quedó guardado. Los mensajes siguen seguros en Kafka mientras tanto.
    @Bean
    public DefaultErrorHandler kafkaErrorHandler(@Value("${app.dead-letter.store-retry-interval-milliseconds}") long retryIntervalMilliseconds) {
        return new DefaultErrorHandler(new FixedBackOff(retryIntervalMilliseconds, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
