from airflow.decorators import task, dag
from airflow.providers.common.sql.sensors.sql import SqlSensor

from typing import Dict
from pendulum import datetime

from pprint import pprint

def _success_criteria(record):
    pprint(record)
    if record is not None:
        return True

def _failure_criteria(record):
    return False


@dag(
    description="DAG in charge of processing partner data",
    start_date=datetime(2024, 2, 7),
    schedule="@hourly",
    max_active_runs=1,
    catchup=False,
)
def fmi_sensor():
    waiting_for_temperature_difference = SqlSensor(
        task_id="waiting_for_temperature_difference",
        conn_id="weather",
        sql="sql/fmi_check_obs_fcast_difference.sql",
        success=_success_criteria,
        failure=False,
        fail_on_empty=False,
        poke_interval=20,
        mode="reschedule",
        timeout=60,
    )

    @task
    def validation() -> Dict[str, str]:
        return {"partner_name": "partner_a", "partner_validation": True}

    @task
    def storing():
        print("storing")

    waiting_for_temperature_difference >> validation() >> storing()


fmi_sensor()
