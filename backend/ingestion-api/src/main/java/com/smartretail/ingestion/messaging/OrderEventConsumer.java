package com.smartretail.ingestion.messaging;

import com.smartretail.ingestion.entity.OrderEventProjection;
import com.smartretail.ingestion.entity.ProcessedEvent;
import com.smartretail.ingestion.event.OrderCreatedEvent;
import com.smartretail.ingestion.repository.OrderEventProjectionRepository;
import com.smartretail.ingestion.repository.ProcessedEventRepository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Component
public class OrderEventConsumer {

    private static final Logger log =
            LoggerFactory.getLogger(OrderEventConsumer.class);

    private final ProcessedEventRepository processedEventRepository;

    private final OrderEventProjectionRepository projectionRepository;

    public OrderEventConsumer(
            ProcessedEventRepository processedEventRepository,
            OrderEventProjectionRepository projectionRepository
    ) {
        this.processedEventRepository =
                processedEventRepository;

        this.projectionRepository =
                projectionRepository;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.orders}"
    )
    @Transactional
    public void consume(
            OrderCreatedEvent event
    ) {

        if (processedEventRepository
                .existsById(event.eventId())) {

            log.info(
                    "event_skipped_duplicate eventId={}",
                    event.eventId()
            );

            return;
        }

        Instant processedAt =
                Instant.now();

        projectionRepository.save(
                new OrderEventProjection(
                        event,
                        processedAt
                )
        );

        processedEventRepository.save(
                new ProcessedEvent(
                        event.eventId(),
                        event.eventType(),
                        processedAt
                )
        );

        log.info(
                "event_consumed eventId={} customerId={} productId={}",
                event.eventId(),
                event.customerId(),
                event.productId()
        );
    }
}