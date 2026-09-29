package com.smartretail.ingestion.dto;

import java.time.Instant;
import java.util.UUID;

public record OrderEventAcceptedResponse(
        UUID eventId,
        String status,
        boolean replayed,
        Instant acceptedAt
) {
}
