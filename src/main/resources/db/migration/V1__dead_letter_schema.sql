-- Cada fila es un mensaje que un servicio no pudo procesar y terminó en un topic *-dlt.
-- Se guarda aquí para revisarlo, reprocesarlo o descartarlo desde la Ops Console,
-- sin depender de la retención de Kafka.
CREATE TABLE dead_letter_messages (
    id BIGSERIAL PRIMARY KEY,

    -- Posición del mensaje dentro del DLT. Es única: evita guardar dos veces el mismo mensaje.
    dlt_topic VARCHAR(255) NOT NULL,
    dlt_partition INT NOT NULL,
    dlt_offset BIGINT NOT NULL,

    -- De dónde venía y quién falló al consumirlo (headers que agrega DeadLetterPublishingRecoverer).
    original_topic VARCHAR(255) NOT NULL,
    original_partition INT,
    original_offset BIGINT,
    consumer_group VARCHAR(255),

    -- Mensaje original, sin modificar, para poder reprocesarlo tal cual.
    message_key VARCHAR(255),
    payload TEXT,

    -- Campos del evento extraídos del payload para poder filtrar.
    event_id VARCHAR(100),
    event_type VARCHAR(100),
    correlation_id VARCHAR(100),
    aggregate_id VARCHAR(100),

    -- Error que lo envió al DLT.
    exception_class VARCHAR(500),
    exception_cause_class VARCHAR(500),
    exception_message TEXT,
    exception_stacktrace TEXT,

    status VARCHAR(30) NOT NULL,
    failed_at TIMESTAMP WITHOUT TIME ZONE,
    received_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    -- Si este mensaje es el resultado de reprocesar otro que volvió a fallar.
    replay_of_id BIGINT REFERENCES dead_letter_messages (id),

    -- Quién lo resolvió (reprocesó o descartó), cuándo y por qué.
    resolved_by VARCHAR(100),
    resolved_at TIMESTAMP WITHOUT TIME ZONE,
    resolution_note VARCHAR(1000),

    CONSTRAINT uq_dead_letter_messages_position UNIQUE (dlt_topic, dlt_partition, dlt_offset)
);

CREATE INDEX idx_dead_letter_messages_status ON dead_letter_messages (status, received_at DESC);
CREATE INDEX idx_dead_letter_messages_original_topic ON dead_letter_messages (original_topic);
CREATE INDEX idx_dead_letter_messages_correlation_id ON dead_letter_messages (correlation_id);
CREATE INDEX idx_dead_letter_messages_aggregate_id ON dead_letter_messages (aggregate_id);
