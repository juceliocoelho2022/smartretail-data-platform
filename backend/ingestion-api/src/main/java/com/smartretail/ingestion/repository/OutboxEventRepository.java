package com.smartretail.ingestion.repository;

import com.smartretail.ingestion.entity.OutboxEvent;
import com.smartretail.ingestion.entity.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
    List<OutboxEvent> findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus status);
}
