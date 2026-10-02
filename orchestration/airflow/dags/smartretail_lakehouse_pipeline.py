import os

import pendulum
from airflow.providers.docker.operators.docker import (
    DockerOperator,
)
from airflow.sdk import dag
from docker.types import Mount


SPARK_IMAGE = os.getenv(
    "SMARTRETAIL_SPARK_IMAGE",
    "smartretail-spark-jobs:0.4",
)

DOCKER_NETWORK = os.getenv(
    "SMARTRETAIL_DOCKER_NETWORK",
    "smartretail-network",
)

S3_ACCESS_KEY = os.getenv(
    "SMARTRETAIL_S3_ACCESS_KEY",
    "smartretail",
)

S3_SECRET_KEY = os.getenv(
    "SMARTRETAIL_S3_SECRET_KEY",
    "smartretail123",
)

COMMON_ENVIRONMENT = {
    "SILVER_PATH": (
        "s3a://smartretail-silver/orders"
    ),
    "GOLD_DAILY_PATH": (
        "s3a://smartretail-gold/orders-daily"
    ),
    "GOLD_SUMMARY_PATH": (
        "s3a://smartretail-gold/orders-summary"
    ),
    "ICEBERG_CATALOG_NAME": "smartretail",
    "ICEBERG_NAMESPACE": "lakehouse",
    "ICEBERG_TABLE": "orders",
    "ICEBERG_WAREHOUSE": (
        "s3a://smartretail-warehouse/iceberg"
    ),
    "ICEBERG_JDBC_URI": (
        "jdbc:postgresql://postgres:5432/"
        "smartretail"
    ),
    "ICEBERG_JDBC_USER": "smartretail",
    "ICEBERG_JDBC_PASSWORD": "smartretail",
    "S3_ENDPOINT": "http://minio:9000",
    "S3_ACCESS_KEY": S3_ACCESS_KEY,
    "S3_SECRET_KEY": S3_SECRET_KEY,
    "S3_REGION": "us-east-1",
    "PYSPARK_PYTHON": "python3",
    "PYSPARK_DRIVER_PYTHON": "python3",
}

HADOOP_PACKAGES = (
    "org.apache.hadoop:"
    "hadoop-aws:3.4.1"
)

ICEBERG_PACKAGES = (
    "org.apache.iceberg:"
    "iceberg-spark-runtime-4.0_2.13:1.11.0,"
    "org.apache.hadoop:"
    "hadoop-aws:3.4.1,"
    "org.postgresql:"
    "postgresql:42.7.13"
)


def spark_command(
    script_name: str,
    packages: str,
) -> list[str]:
    return [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[*]",
        "--conf",
        "spark.jars.ivy=/tmp/.ivy2",
        "--packages",
        packages,
        (
            "/opt/spark/work-dir/src/"
            "main/python/"
            f"{script_name}"
        ),
    ]


def spark_task(
    *,
    task_id: str,
    script_name: str,
    packages: str,
) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=spark_command(
            script_name,
            packages,
        ),
        docker_url=(
            "unix://var/run/docker.sock"
        ),
        network_mode=DOCKER_NETWORK,
        environment=COMMON_ENVIRONMENT,
        mounts=[
            Mount(
                source=(
                    "smartretail-spark-ivy"
                ),
                target="/tmp/.ivy2",
                type="volume",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        force_pull=False,
    )


@dag(
    dag_id=(
        "smartretail_lakehouse_pipeline"
    ),
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    tags=[
        "smartretail",
        "v0.4",
        "lakehouse",
        "data-quality",
    ],
)
def smartretail_lakehouse_pipeline():

    data_quality = spark_task(
        task_id="silver_data_quality",
        script_name="data_quality_app.py",
        packages=HADOOP_PACKAGES,
    )

    gold = spark_task(
        task_id="build_gold",
        script_name="gold_app.py",
        packages=HADOOP_PACKAGES,
    )

    iceberg = spark_task(
        task_id="refresh_iceberg",
        script_name="iceberg_app.py",
        packages=ICEBERG_PACKAGES,
    )

    validate = spark_task(
        task_id="post_load_validation",
        script_name=(
            "post_load_validation.py"
        ),
        packages=ICEBERG_PACKAGES,
    )

    (
        data_quality
        >> gold
        >> iceberg
        >> validate
    )


smartretail_lakehouse_pipeline()
