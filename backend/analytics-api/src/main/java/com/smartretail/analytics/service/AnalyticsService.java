package com.smartretail.analytics.service;

import com.smartretail.analytics.api.DailySalesResponse;
import com.smartretail.analytics.api.SalesSummaryResponse;
import com.smartretail.analytics.repository.AnalyticsRepository;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.util.List;
import java.util.Locale;

@Service
public class AnalyticsService {

    private final AnalyticsRepository analyticsRepository;

    public AnalyticsService(
            AnalyticsRepository analyticsRepository
    ) {
        this.analyticsRepository = analyticsRepository;
    }

    public SalesSummaryResponse getSummary() {
        return analyticsRepository
                .findSummary()
                .orElseThrow(
                        () -> new ResponseStatusException(
                                HttpStatus.NOT_FOUND,
                                "Analytics summary is not available yet."
                        )
                );
    }

    public List<DailySalesResponse> getDailySales(
            LocalDate from,
            LocalDate to,
            String channel,
            String location,
            int limit
    ) {
        if (
                from != null
                && to != null
                && from.isAfter(to)
        ) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "'from' must be before or equal to 'to'."
            );
        }

        return analyticsRepository.findDailySales(
                from,
                to,
                normalize(channel),
                normalize(location),
                limit
        );
    }

    private String normalize(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        return value
                .strip()
                .toUpperCase(Locale.ROOT);
    }
}
