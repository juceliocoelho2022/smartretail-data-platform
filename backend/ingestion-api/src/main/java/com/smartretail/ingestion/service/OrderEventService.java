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
            return new OrderEventAcceptance(
                    existing.get().getEventId(),
                    true,
                    existing.get().getCreatedAt()
            );
        }

        Instant now = Instant.now();
        UUID eventId = UUID.randomUUID();

        var event = new OrderCreatedEvent(
                eventId,
                "ORDER_CREATED",
                1,
                now,
                "ingestion-api",
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
                event.eventType(),
                serialize(event),
                now
        ));

        return new OrderEventAcceptance(eventId, false, now);
    }

    private String serialize(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Unable to serialize order event", ex);
        }
    }
}
