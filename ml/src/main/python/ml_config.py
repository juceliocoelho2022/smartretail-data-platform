import os


ML_HISTORY_SILVER_PATH = os.getenv(
    "ML_HISTORY_SILVER_PATH",
    "s3a://smartretail-silver/orders-ml-history",
)

PRODUCT_DEMAND_GOLD_PATH = os.getenv(
    "PRODUCT_DEMAND_GOLD_PATH",
    "s3a://smartretail-gold/product-demand-daily",
)

ML_FEATURES_PATH = os.getenv(
    "ML_FEATURES_PATH",
    "s3a://smartretail-gold/ml/demand-features",
)

ML_TRAINING_MANIFEST_PATH = os.getenv(
    "ML_TRAINING_MANIFEST_PATH",
    "s3a://smartretail-gold/ml/training-manifest",
)

MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    "http://mlflow:5000",
)

MODEL_NAME = os.getenv(
    "MODEL_NAME",
    "smartretail-demand-forecast",
)

FORECAST_HORIZON_DAYS = int(
    os.getenv("FORECAST_HORIZON_DAYS", "7")
)

ANALYTICS_JDBC_URL = os.getenv(
    "ANALYTICS_JDBC_URL",
    "jdbc:postgresql://postgres:5432/smartretail",
)

ANALYTICS_JDBC_USER = os.getenv(
    "ANALYTICS_JDBC_USER",
    "smartretail",
)

ANALYTICS_JDBC_PASSWORD = os.getenv(
    "ANALYTICS_JDBC_PASSWORD",
    "smartretail",
)

ANALYTICS_PG_DSN = os.getenv(
    "ANALYTICS_PG_DSN",
    "dbname=smartretail user=smartretail password=smartretail host=postgres port=5432",
)

FORECAST_TARGET_TABLE = os.getenv(
    "FORECAST_TARGET_TABLE",
    "analytics.demand_forecast",
)

FORECAST_STAGING_TABLE = os.getenv(
    "FORECAST_STAGING_TABLE",
    "analytics.demand_forecast_staging",
)

ML_HISTORY_START_DATE = os.getenv(
    "ML_HISTORY_START_DATE",
    "2025-10-01",
)

ML_HISTORY_DAYS = int(
    os.getenv("ML_HISTORY_DAYS", "365")
)

ML_PRODUCT_COUNT = int(
    os.getenv("ML_PRODUCT_COUNT", "30")
)

ML_RANDOM_SEED = int(
    os.getenv("ML_RANDOM_SEED", "20261002")
)

S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT",
    "http://minio:9000",
)

S3_ACCESS_KEY = os.getenv(
    "S3_ACCESS_KEY",
    "smartretail",
)

S3_SECRET_KEY = os.getenv(
    "S3_SECRET_KEY",
    "smartretail123",
)

S3_REGION = os.getenv(
    "S3_REGION",
    "us-east-1",
)
