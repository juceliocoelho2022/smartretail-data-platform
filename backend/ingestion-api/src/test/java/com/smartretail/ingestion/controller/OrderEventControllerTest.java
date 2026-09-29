package com.smartretail.ingestion.controller;

import com.smartretail.ingestion.service.OrderEventAcceptance;
import com.smartretail.ingestion.service.OrderEventService;
import com.smartretail.ingestion.service.OrderQueryService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(OrderEventController.class)
class OrderEventControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    OrderEventService orderEventService;

    @MockitoBean
    OrderQueryService orderQueryService;

    @Test
    void shouldAcceptValidOrderEvent() throws Exception {
        UUID eventId = UUID.randomUUID();
        when(orderEventService.accept(any(), eq("req-123")))
                .thenReturn(new OrderEventAcceptance(
                        eventId, false, Instant.parse("2026-09-29T20:00:00Z")
                ));

        mockMvc.perform(post("/api/v1/events/orders")
                        .header("Idempotency-Key", "req-123")
                        .contentType("application/json")
                        .content("""
                                {
                                  "customerId":"CUS-1",
                                  "productId":"PROD-1",
                                  "quantity":2,
                                  "unitPrice":99.90,
                                  "channel":"WEB",
                                  "location":"SAO_PAULO"
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.eventId").value(eventId.toString()))
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.replayed").value(false));
    }
}
