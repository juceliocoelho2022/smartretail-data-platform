package com.smartretail.analytics;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ForecastVintageMigrationTest {

    @Test
    void migrationDefinesFourColumnForecastVintagePrimaryKey() throws Exception {
        var migration = Path.of(
                "src/main/resources/db/migration/V3__forecast_vintage_key.sql"
        );

        assertTrue(Files.exists(migration), "V3 forecast vintage migration must exist");

        var sql = Files.readString(migration)
                .replaceAll("\\s+", " ")
                .toLowerCase();

        assertTrue(sql.contains(
                "primary key (product_id, forecast_date, model_version, training_cutoff_date)"
        ));
    }
}
