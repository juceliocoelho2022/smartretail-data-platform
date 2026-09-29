package com.smartretail.ingestion.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderProjectionResponse(
        UUID eventId,
        String customerId,
        String productId,
        Integer quantity,
        BigDecimal unitPrice,
        String channel,
        String location,
        Instant occurredAt,
        Instant processedAt
) {
}
