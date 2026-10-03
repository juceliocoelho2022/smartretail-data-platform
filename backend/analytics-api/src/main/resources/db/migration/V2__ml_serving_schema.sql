CREATE TABLE IF NOT EXISTS demand_forecast (
    product_id VARCHAR(100) NOT NULL,
    forecast_date DATE NOT NULL,
    predicted_units DOUBLE PRECISION NOT NULL,
    model_name VARCHAR(200) NOT NULL,
    model_version VARCHAR(100) NOT NULL,
    training_cutoff_date DATE NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (product_id, forecast_date, model_version)
);

CREATE TABLE IF NOT EXISTS demand_forecast_staging (
    product_id VARCHAR(100) NOT NULL,
    forecast_date DATE NOT NULL,
    predicted_units DOUBLE PRECISION NOT NULL,
    model_name VARCHAR(200) NOT NULL,
    model_version VARCHAR(100) NOT NULL,
    training_cutoff_date DATE NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_demand_forecast_product
    ON demand_forecast (product_id);

CREATE INDEX IF NOT EXISTS idx_demand_forecast_date
    ON demand_forecast (forecast_date);

CREATE TABLE IF NOT EXISTS sales_anomaly (
    event_date DATE NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    actual_units DOUBLE PRECISION NOT NULL,
    expected_units DOUBLE PRECISION NOT NULL,
    residual DOUBLE PRECISION NOT NULL,
    anomaly_score DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    model_version VARCHAR(100) NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_date, product_id, model_version)
);

CREATE TABLE IF NOT EXISTS sales_anomaly_staging (
    event_date DATE NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    actual_units DOUBLE PRECISION NOT NULL,
    expected_units DOUBLE PRECISION NOT NULL,
    residual DOUBLE PRECISION NOT NULL,
    anomaly_score DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN NOT NULL,
    model_version VARCHAR(100) NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sales_anomaly_product
    ON sales_anomaly (product_id);

CREATE INDEX IF NOT EXISTS idx_sales_anomaly_date
    ON sales_anomaly (event_date);

CREATE INDEX IF NOT EXISTS idx_sales_anomaly_flag
    ON sales_anomaly (is_anomaly);
