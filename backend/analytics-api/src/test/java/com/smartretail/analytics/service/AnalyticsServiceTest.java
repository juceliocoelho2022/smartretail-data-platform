package com.smartretail.analytics.service;

import com.smartretail.analytics.api.DailySalesResponse;
import com.smartretail.analytics.api.SalesSummaryResponse;
import com.smartretail.analytics.repository.AnalyticsRepository;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AnalyticsServiceTest {

    private final AnalyticsRepository repository =
            mock(AnalyticsRepository.class);

    private final AnalyticsService service =
            new AnalyticsService(repository);

    @Test
    void shouldReturnSummary() {
        var timestamp = OffsetDateTime.of(
                2026,
                10,
                2,
                17,
                0,
                0,
                0,
                ZoneOffset.UTC
        );

        var summary = new SalesSummaryResponse(
                5,
                14,
                new BigDecimal("2918.60"),
                new BigDecimal("583.72"),
                4,
                4,
                timestamp,
                timestamp
        );

        when(repository.findSummary())
                .thenReturn(Optional.of(summary));

        assertEquals(
                summary,
                service.getSummary()
        );
    }

    @Test
    void shouldNormalizeFilters() {
        when(
                repository.findDailySales(
                        null,
                        null,
                        "WEB",
                        "SAO_PAULO",
                        100
                )
        ).thenReturn(List.of());

        service.getDailySales(
                null,
                null,
                " web ",
                " sao_paulo ",
                100
        );

        verify(repository).findDailySales(
                null,
                null,
                "WEB",
                "SAO_PAULO",
                100
        );
    }

    @Test
    void shouldRejectInvalidDateRange() {
        var from = LocalDate.of(
                2026,
                10,
                3
        );

        var to = LocalDate.of(
                2026,
                10,
                2
        );

        assertThrows(
                ResponseStatusException.class,
                () -> service.getDailySales(
                        from,
                        to,
                        null,
                        null,
                        100
                )
        );
    }
}
