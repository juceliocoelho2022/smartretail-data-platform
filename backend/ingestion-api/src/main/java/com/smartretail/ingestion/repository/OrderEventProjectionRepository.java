package com.smartretail.ingestion.repository;

import com.smartretail.ingestion.entity.OrderEventProjection;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface OrderEventProjectionRepository
        extends JpaRepository<OrderEventProjection, UUID> {
}