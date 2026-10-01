package com.smartretail.ingestion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartretail.ingestion.dto.OrderEventRequest;
import com.smartretail.ingestion.entity.IdempotencyRecord;
import com.smartretail.ingestion.entity.OutboxEvent;
import com.smartretail.ingestion.event.OrderCreatedEvent;
import com.smartretail.ingestion.repository.IdempotencyRecordRepository;
import com.smartretail.ingestion.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Service
public class OrderEventService {

    private static final String EVENT_TYPE = "ORDER_CREATED";
    private static final String PRODUCER = "smartretail-ingestion-api";

    private final IdempotencyRecordRepository idempotencyRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public OrderEventService(IdempotencyRecordRepository idempotencyRepository,
                             OutboxEventRepository outboxRepository,
                             ObjectMapper objectMapper) {
        this.idempotencyRepository = idempotencyRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public OrderEventAcceptance accept(OrderEventRequest request, String idempotencyKey) {
        var existing = idempotencyRepository.findById(idempotencyKey);

        if (existing.isPresent()) {
            var record = existing.get();
            return new OrderEventAcceptance(record.getEventId(), true, record.getCreatedAt());
        }

        Instant now = Instant.now();
        UUID eventId = UUID.randomUUID();

        var event = new OrderCreatedEvent(
                eventId,
                EVENT_TYPE,
                1,
                now,
                PRODUCER,
                UUID.randomUUID().toString(),
                request.customerId(),
                request.productId(),
                request.quantity(),
                request.unitPrice(),
                request.channel(),
                request.location()
        );

        idempotencyRepository.save(new IdempotencyRecord(idempotencyKey, eventId, now));

        outboxRepository.save(new OutboxEvent(
                eventId,
                "ORDER",
                eventId.toString(),
                EVENT_TYPE,
                serialize(event),
                now
        ));

        return new OrderEventAcceptance(eventId, false, now);
    }

    private String serialize(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Unable to serialize order event", e);
        }
    }
}
