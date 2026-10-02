package com.smartretail.analytics.api;

import com.smartretail.analytics.service.AnalyticsService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

@Validated
@RestController
@RequestMapping("/api/v1/analytics")
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    public AnalyticsController(
            AnalyticsService analyticsService
    ) {
        this.analyticsService = analyticsService;
    }

    @GetMapping("/summary")
    public SalesSummaryResponse getSummary() {
        return analyticsService.getSummary();
    }

    @GetMapping("/sales/daily")
    public List<DailySalesResponse> getDailySales(
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
            LocalDate from,

            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE)
            LocalDate to,

            @RequestParam(required = false)
            String channel,

            @RequestParam(required = false)
            String location,

            @RequestParam(defaultValue = "100")
            @Min(1)
            @Max(1000)
            int limit
    ) {
        return analyticsService.getDailySales(
                from,
                to,
                channel,
                location,
                limit
        );
    }
}
