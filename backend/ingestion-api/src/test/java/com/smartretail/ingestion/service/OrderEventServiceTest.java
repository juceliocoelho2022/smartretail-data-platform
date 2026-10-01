package com.smartretail.ingestion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartretail.ingestion.dto.OrderEventRequest;
import com.smartretail.ingestion.entity.IdempotencyRecord;
import com.smartretail.ingestion.entity.OutboxEvent;
import com.smartretail.ingestion.repository.IdempotencyRecordRepository;
import com.smartretail.ingestion.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderEventServiceTest {

    @Mock
    private IdempotencyRecordRepository idempotencyRepository;

    @Mock
    private OutboxEventRepository outboxRepository;

    private OrderEventService service;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();

        service = new OrderEventService(
                idempotencyRepository,
                outboxRepository,
                objectMapper
        );
    }

    @Test
    void shouldCreateNewOrderEventAndOutboxRecord() {

        var request = new OrderEventRequest(
                "CUST-001",
                "PROD-001",
                2,
                new BigDecimal("149.90"),
                "WEB",
                "SAO_PAULO"
        );

        String idempotencyKey = "order-test-001";

        when(idempotencyRepository.findById(idempotencyKey))
                .thenReturn(Optional.empty());

        var result = service.accept(request, idempotencyKey);

        assertThat(result.eventId()).isNotNull();
        assertThat(result.replayed()).isFalse();
        assertThat(result.acceptedAt()).isNotNull();

        ArgumentCaptor<IdempotencyRecord> idempotencyCaptor =
                ArgumentCaptor.forClass(IdempotencyRecord.class);

        verify(idempotencyRepository)
                .save(idempotencyCaptor.capture());

        var savedIdempotency = idempotencyCaptor.getValue();

        assertThat(savedIdempotency.getIdempotencyKey())
                .isEqualTo(idempotencyKey);

        assertThat(savedIdempotency.getEventId())
                .isEqualTo(result.eventId());

        ArgumentCaptor<OutboxEvent> outboxCaptor =
                ArgumentCaptor.forClass(OutboxEvent.class);

        verify(outboxRepository)
                .save(outboxCaptor.capture());

        var savedOutbox = outboxCaptor.getValue();

        assertThat(savedOutbox.getId())
                .isEqualTo(result.eventId());

        assertThat(savedOutbox.getAggregateId())
                .isEqualTo(result.eventId().toString());

        assertThat(savedOutbox.getPayload())
                .contains("\"customerId\":\"CUST-001\"");

        assertThat(savedOutbox.getPayload())
                .contains("\"productId\":\"PROD-001\"");
    }

    @Test
    void shouldReplayExistingEventForSameIdempotencyKey() {

        String idempotencyKey = "order-existing";

        UUID eventId =
                UUID.fromString(
                        "21e31d5c-0c1a-41b5-9ff8-0772e44c8cfd"
                );

        Instant createdAt =
                Instant.parse("2026-09-30T22:08:59Z");

        var existing =
                new IdempotencyRecord(
                        idempotencyKey,
                        eventId,
                        createdAt
                );

        when(idempotencyRepository.findById(idempotencyKey))
                .thenReturn(Optional.of(existing));

        var request = new OrderEventRequest(
                "CUST-001",
                "PROD-001",
                2,
                new BigDecimal("149.90"),
                "WEB",
                "SAO_PAULO"
        );

        var result =
                service.accept(request, idempotencyKey);

        assertThat(result.eventId())
                .isEqualTo(eventId);

        assertThat(result.replayed())
                .isTrue();

        assertThat(result.acceptedAt())
                .isEqualTo(createdAt);

        verify(idempotencyRepository, never())
                .save(any());

        verify(outboxRepository, never())
                .save(any());
    }
}