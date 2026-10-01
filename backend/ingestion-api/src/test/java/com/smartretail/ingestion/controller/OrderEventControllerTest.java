package com.smartretail.ingestion.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartretail.ingestion.dto.OrderEventRequest;
import com.smartretail.ingestion.dto.OrderProjectionResponse;
import com.smartretail.ingestion.service.OrderEventAcceptance;
import com.smartretail.ingestion.service.OrderEventService;
import com.smartretail.ingestion.service.OrderQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class OrderEventControllerTest {

    private OrderEventService orderEventService;
    private OrderQueryService orderQueryService;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {

        orderEventService =
                mock(OrderEventService.class);

        orderQueryService =
                mock(OrderQueryService.class);

        var controller =
                new OrderEventController(
                        orderEventService,
                        orderQueryService
                );

        mockMvc =
                MockMvcBuilders
                        .standaloneSetup(controller)
                        .build();

        objectMapper = new ObjectMapper();
        objectMapper.findAndRegisterModules();
    }

    @Test
    void shouldAcceptOrderEvent() throws Exception {

        UUID eventId = UUID.randomUUID();

        Instant acceptedAt =
                Instant.parse("2026-09-30T22:08:59Z");

        when(
                orderEventService.accept(
                        any(OrderEventRequest.class),
                        eq("idem-001")
                )
        ).thenReturn(
                new OrderEventAcceptance(
                        eventId,
                        false,
                        acceptedAt
                )
        );

        var request = new OrderEventRequest(
                "CUST-001",
                "PROD-001",
                2,
                new BigDecimal("149.90"),
                "WEB",
                "SAO_PAULO"
        );

        mockMvc.perform(
                        post("/api/v1/events/orders")
                                .header(
                                        "Idempotency-Key",
                                        "idem-001"
                                )
                                .contentType(
                                        MediaType.APPLICATION_JSON
                                )
                                .content(
                                        objectMapper.writeValueAsString(
                                                request
                                        )
                                )
                )
                .andExpect(
                        status().isAccepted()
                )
                .andExpect(
                        header().string(
                                "Location",
                                "/api/v1/events/orders/" + eventId
                        )
                )
                .andExpect(
                        jsonPath("$.eventId")
                                .value(eventId.toString())
                )
                .andExpect(
                        jsonPath("$.status")
                                .value("ACCEPTED")
                )
                .andExpect(
                        jsonPath("$.replayed")
                                .value(false)
                );
    }

    @Test
    void shouldReturnOrderProjection() throws Exception {

        UUID eventId = UUID.randomUUID();

        var response =
                new OrderProjectionResponse(
                        eventId,
                        "CUST-001",
                        "PROD-001",
                        2,
                        new BigDecimal("149.90"),
                        "WEB",
                        "SAO_PAULO",
                        Instant.parse(
                                "2026-09-30T22:08:59Z"
                        ),
                        Instant.parse(
                                "2026-09-30T22:09:00Z"
                        )
                );

        when(orderQueryService.find(eventId))
                .thenReturn(Optional.of(response));

        mockMvc.perform(
                        get(
                                "/api/v1/events/orders/{eventId}",
                                eventId
                        )
                )
                .andExpect(status().isOk())
                .andExpect(
                        jsonPath("$.eventId")
                                .value(eventId.toString())
                )
                .andExpect(
                        jsonPath("$.customerId")
                                .value("CUST-001")
                )
                .andExpect(
                        jsonPath("$.productId")
                                .value("PROD-001")
                )
                .andExpect(
                        jsonPath("$.quantity")
                                .value(2)
                );
    }

    @Test
    void shouldReturn404WhenProjectionDoesNotExist()
            throws Exception {

        UUID eventId = UUID.randomUUID();

        when(orderQueryService.find(eventId))
                .thenReturn(Optional.empty());

        mockMvc.perform(
                        get(
                                "/api/v1/events/orders/{eventId}",
                                eventId
                        )
                )
                .andExpect(
                        status().isNotFound()
                );
    }
}