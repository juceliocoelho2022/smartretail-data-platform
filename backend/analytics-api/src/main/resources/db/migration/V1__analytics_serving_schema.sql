CREATE TABLE IF NOT EXISTS sales_summary (
    id SMALLINT PRIMARY KEY,
    total_orders BIGINT NOT NULL,
    total_items BIGINT NOT NULL,
    total_revenue NUMERIC(19, 2) NOT NULL,
    average_order_value NUMERIC(19, 2) NOT NULL,
    unique_customers BIGINT NOT NULL,
    unique_products BIGINT NOT NULL,
    gold_processed_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_sales_summary_singleton CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS sales_daily (
    event_date DATE NOT NULL,
    channel VARCHAR(50) NOT NULL,
    location VARCHAR(100) NOT NULL,
    total_orders BIGINT NOT NULL,
    total_items BIGINT NOT NULL,
    total_revenue NUMERIC(19, 2) NOT NULL,
    average_order_value NUMERIC(19, 2) NOT NULL,
    gold_processed_at TIMESTAMPTZ NOT NULL,
    refreshed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_date, channel, location)
);

CREATE INDEX IF NOT EXISTS idx_sales_daily_event_date
    ON sales_daily (event_date);

CREATE INDEX IF NOT EXISTS idx_sales_daily_channel
    ON sales_daily (channel);

CREATE INDEX IF NOT EXISTS idx_sales_daily_location
    ON sales_daily (location);
