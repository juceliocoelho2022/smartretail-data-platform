package com.smartretail.ingestion.entity;

import com.smartretail.ingestion.event.OrderCreatedEvent;
import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "order_event_projection")
public class OrderEventProjection {

    @Id
    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "customer_id", nullable = false, length = 80)
    private String customerId;

    @Column(name = "product_id", nullable = false, length = 80)
    private String productId;

    @Column(nullable = false)
    private Integer quantity;

    @Column(name = "unit_price", nullable = false, precision = 19, scale = 2)
    private BigDecimal unitPrice;

    @Column(nullable = false, length = 30)
    private String channel;

    @Column(nullable = false, length = 80)
    private String location;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;

    protected OrderEventProjection() {
    }

    public OrderEventProjection(OrderCreatedEvent event, Instant processedAt) {
        this.eventId = event.eventId();
        this.customerId = event.customerId();
        this.productId = event.productId();
        this.quantity = event.quantity();
        this.unitPrice = event.unitPrice();
        this.channel = event.channel();
        this.location = event.location();
        this.occurredAt = event.occurredAt();
        this.processedAt = processedAt;
    }

    public UUID getEventId() { return eventId; }
    public String getCustomerId() { return customerId; }
    public String getProductId() { return productId; }
    public Integer getQuantity() { return quantity; }
    public BigDecimal getUnitPrice() { return unitPrice; }
    public String getChannel() { return channel; }
    public String getLocation() { return location; }
    public Instant getOccurredAt() { return occurredAt; }
    public Instant getProcessedAt() { return processedAt; }
}
