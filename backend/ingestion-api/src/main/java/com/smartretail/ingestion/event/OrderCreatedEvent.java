package com.smartretail.ingestion.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        String eventType,
        int eventVersion,
        Instant occurredAt,
        String producer,
        String correlationId,
        String customerId,
        String productId,
        Integer quantity,
        BigDecimal unitPrice,
        String channel,
        String location
) {}
