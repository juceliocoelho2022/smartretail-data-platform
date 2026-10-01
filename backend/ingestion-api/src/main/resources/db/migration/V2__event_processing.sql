CREATE TABLE processed_event
(
    event_id UUID PRIMARY KEY,

    event_type VARCHAR(100) NOT NULL,

    processed_at TIMESTAMPTZ NOT NULL
);


CREATE TABLE order_event_projection
(
    event_id UUID PRIMARY KEY,

    customer_id VARCHAR(80) NOT NULL,

    product_id VARCHAR(80) NOT NULL,

    quantity INTEGER NOT NULL
        CHECK (quantity > 0),

    unit_price NUMERIC(19, 2) NOT NULL
        CHECK (unit_price > 0),

    channel VARCHAR(30) NOT NULL,

    location VARCHAR(80) NOT NULL,

    occurred_at TIMESTAMPTZ NOT NULL,

    processed_at TIMESTAMPTZ NOT NULL
);


CREATE INDEX idx_order_event_projection_processed_at
    ON order_event_projection(processed_at DESC);


CREATE INDEX idx_order_event_projection_customer
    ON order_event_projection(customer_id);


CREATE INDEX idx_order_event_projection_product
    ON order_event_projection(product_id);
