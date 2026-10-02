package com.smartretail.analytics.api;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

public record SalesSummaryResponse(
        long totalOrders,
        long totalItems,
        BigDecimal totalRevenue,
        BigDecimal averageOrderValue,
        long uniqueCustomers,
        long uniqueProducts,
        OffsetDateTime goldProcessedAt,
        OffsetDateTime refreshedAt
) {
}
