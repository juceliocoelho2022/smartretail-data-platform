package com.smartretail.ingestion.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartretail.ingestion.entity.OutboxStatus;
import com.smartretail.ingestion.event.OrderCreatedEvent;
import com.smartretail.ingestion.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    private final OutboxEventRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String ordersTopic;

    public OutboxPublisher(OutboxEventRepository repository,
                           KafkaTemplate<String, Object> kafkaTemplate,
                           ObjectMapper objectMapper,
                           @Value("${app.kafka.topics.orders}") String ordersTopic) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.ordersTopic = ordersTopic;
    }

    @Scheduled(fixedDelayString = "${app.outbox.fixed-delay-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        var events = repository.findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        for (var outbox : events) {
            try {
                var event = objectMapper.readValue(outbox.getPayload(), OrderCreatedEvent.class);
                kafkaTemplate.send(ordersTopic, outbox.getAggregateId(), event).get(5, TimeUnit.SECONDS);
                outbox.markPublished(Instant.now());
                log.info("event_published eventId={} topic={}", outbox.getId(), ordersTopic);
            } catch (Exception ex) {
                outbox.markFailure(ex.getMessage());
                log.error("event_publish_failed eventId={} attempts={}",
                        outbox.getId(), outbox.getAttempts(), ex);
            }
        }
    }
}
