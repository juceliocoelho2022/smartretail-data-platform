package com.smartretail.ingestion.entity;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "idempotency_record")
public class IdempotencyRecord {

    @Id
    @Column(name = "idempotency_key", nullable = false, length = 120)
    private String idempotencyKey;

    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected IdempotencyRecord() {}

    public IdempotencyRecord(String idempotencyKey, UUID eventId, Instant createdAt) {
        this.idempotencyKey = idempotencyKey;
        this.eventId = eventId;
        this.createdAt = createdAt;
    }

    public String getIdempotencyKey() { return idempotencyKey; }
    public UUID getEventId() { return eventId; }
    public Instant getCreatedAt() { return createdAt; }
}
