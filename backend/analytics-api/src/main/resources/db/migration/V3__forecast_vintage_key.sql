ALTER TABLE analytics.demand_forecast
    DROP CONSTRAINT IF EXISTS demand_forecast_pkey;

ALTER TABLE analytics.demand_forecast
    ADD CONSTRAINT demand_forecast_pkey
    PRIMARY KEY (
        product_id,
        forecast_date,
        model_version,
        training_cutoff_date
    );
