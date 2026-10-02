package com.smartretail.analytics.repository;

import com.smartretail.analytics.api.DailySalesResponse;
import com.smartretail.analytics.api.SalesSummaryResponse;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public class AnalyticsRepository {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public AnalyticsRepository(
            NamedParameterJdbcTemplate jdbcTemplate
    ) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public Optional<SalesSummaryResponse> findSummary() {
        var sql = """
                SELECT
                    total_orders,
                    total_items,
                    total_revenue,
                    average_order_value,
                    unique_customers,
                    unique_products,
                    gold_processed_at,
                    refreshed_at
                FROM analytics.sales_summary
                WHERE id = 1
                """;

        return jdbcTemplate.query(
                sql,
                new MapSqlParameterSource(),
                (rs, rowNum) -> new SalesSummaryResponse(
                        rs.getLong("total_orders"),
                        rs.getLong("total_items"),
                        rs.getBigDecimal("total_revenue"),
                        rs.getBigDecimal("average_order_value"),
                        rs.getLong("unique_customers"),
                        rs.getLong("unique_products"),
                        rs.getObject(
                                "gold_processed_at",
                                OffsetDateTime.class
                        ),
                        rs.getObject(
                                "refreshed_at",
                                OffsetDateTime.class
                        )
                )
        ).stream().findFirst();
    }

    public List<DailySalesResponse> findDailySales(
            LocalDate from,
            LocalDate to,
            String channel,
            String location,
            int limit
    ) {
        var sql = new StringBuilder("""
                SELECT
                    event_date,
                    channel,
                    location,
                    total_orders,
                    total_items,
                    total_revenue,
                    average_order_value,
                    gold_processed_at,
                    refreshed_at
                FROM analytics.sales_daily
                WHERE 1 = 1
                """);

        var params = new MapSqlParameterSource();

        if (from != null) {
            sql.append(" AND event_date >= :from");
            params.addValue("from", from);
        }

        if (to != null) {
            sql.append(" AND event_date <= :to");
            params.addValue("to", to);
        }

        if (channel != null) {
            sql.append(" AND channel = :channel");
            params.addValue("channel", channel);
        }

        if (location != null) {
            sql.append(" AND location = :location");
            params.addValue("location", location);
        }

        sql.append(
                " ORDER BY event_date DESC, channel, location"
        );
        sql.append(" LIMIT :limit");
        params.addValue("limit", limit);

        return jdbcTemplate.query(
                sql.toString(),
                params,
                (rs, rowNum) -> new DailySalesResponse(
                        rs.getObject(
                                "event_date",
                                LocalDate.class
                        ),
                        rs.getString("channel"),
                        rs.getString("location"),
                        rs.getLong("total_orders"),
                        rs.getLong("total_items"),
                        rs.getBigDecimal("total_revenue"),
                        rs.getBigDecimal("average_order_value"),
                        rs.getObject(
                                "gold_processed_at",
                                OffsetDateTime.class
                        ),
                        rs.getObject(
                                "refreshed_at",
                                OffsetDateTime.class
                        )
                )
        );
    }
}
