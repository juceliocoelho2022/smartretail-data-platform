package com.smartretail.analytics.api;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;

public record DailySalesResponse(
        LocalDate eventDate,
        String channel,
        String location,
        long totalOrders,
        long totalItems,
        BigDecimal totalRevenue,
        BigDecimal averageOrderValue,
        OffsetDateTime goldProcessedAt,
        OffsetDateTime refreshedAt
) {
}
