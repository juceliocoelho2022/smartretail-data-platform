package com.smartretail.ingestion.controller;

import com.smartretail.ingestion.dto.OrderEventAcceptedResponse;
import com.smartretail.ingestion.dto.OrderEventRequest;
import com.smartretail.ingestion.dto.OrderProjectionResponse;
import com.smartretail.ingestion.service.OrderEventService;
import com.smartretail.ingestion.service.OrderQueryService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.UUID;

@Validated
@RestController
@RequestMapping("/api/v1/events/orders")
public class OrderEventController {

    private final OrderEventService orderEventService;
    private final OrderQueryService orderQueryService;

    public OrderEventController(OrderEventService orderEventService, OrderQueryService orderQueryService) {
        this.orderEventService = orderEventService;
        this.orderQueryService = orderQueryService;
    }

    @PostMapping
    public ResponseEntity<OrderEventAcceptedResponse> create(
            @RequestHeader("Idempotency-Key") @NotBlank String idempotencyKey,
            @Valid @RequestBody OrderEventRequest request) {

        var result = orderEventService.accept(request, idempotencyKey);
        var response = new OrderEventAcceptedResponse(
                result.eventId(),
                "ACCEPTED",
                result.replayed(),
                result.acceptedAt()
        );

        return ResponseEntity.accepted()
                .location(URI.create("/api/v1/events/orders/" + result.eventId()))
                .body(response);
    }

    @GetMapping("/{eventId}")
    public ResponseEntity<OrderProjectionResponse> find(@PathVariable UUID eventId) {
        return orderQueryService.find(eventId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
