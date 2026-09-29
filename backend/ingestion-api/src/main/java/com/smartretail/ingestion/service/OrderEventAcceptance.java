package com.smartretail.ingestion.service;

import java.time.Instant;
import java.util.UUID;

public record OrderEventAcceptance(UUID eventId, boolean replayed, Instant acceptedAt) {
}
