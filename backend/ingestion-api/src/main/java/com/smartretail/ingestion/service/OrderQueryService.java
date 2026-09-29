package com.smartretail.ingestion.service;

import com.smartretail.ingestion.dto.OrderProjectionResponse;
import com.smartretail.ingestion.repository.OrderEventProjectionRepository;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.UUID;

@Service
public class OrderQueryService {

    private final OrderEventProjectionRepository repository;

    public OrderQueryService(OrderEventProjectionRepository repository) {
        this.repository = repository;
    }

    public Optional<OrderProjectionResponse> find(UUID eventId) {
        return repository.findById(eventId).map(entity -> new OrderProjectionResponse(
                entity.getEventId(),
                entity.getCustomerId(),
                entity.getProductId(),
                entity.getQuantity(),
                entity.getUnitPrice(),
                entity.getChannel(),
                entity.getLocation(),
                entity.getOccurredAt(),
                entity.getProcessedAt()
        ));
    }
}
