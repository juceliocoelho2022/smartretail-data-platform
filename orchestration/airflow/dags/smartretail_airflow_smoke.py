import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="smartretail_airflow_smoke",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    tags=[
        "smartretail",
        "v0.4",
        "smoke",
    ],
)
def smartretail_airflow_smoke():

    @task
    def verify_airflow_runtime():
        message = (
            "SmartRetail Airflow v0.4 "
            "runtime is operational."
        )

        print(
            message
        )

        return message

    verify_airflow_runtime()


smartretail_airflow_smoke()
