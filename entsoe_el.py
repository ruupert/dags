import datetime
import pendulum
import logging
from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance

logger = logging.getLogger(__name__)

@dag(
    schedule="25 13 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=45),
    },
    tags=["electricity"],
)

def entsoe_el():
    hook = PostgresHook(postgres_conn_id='electricity')

    @task(
        queue="cves",
        task_id="dag_context"
    )
    def get_dag_context(ti=None, dag_run=None, ds=None):
        logger.info(f"backfill date: {ds}, run_type: {dag_run.run_type}")
        return {"run_type": dag_run.run_type, "ds": ds}

    def execute_query_with_conn_obj(query, datatuple, hook):
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(query, datatuple)
        conn.commit()


    create_electricity_tables = SQLExecuteQueryOperator(
        task_id="create_electricity_tables",
        conn_id="electricity",
        sql="""
            CREATE TABLE IF NOT EXISTS price (date timestamp NOT NULL, val REAL NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_price_date ON price (date);
            CREATE TABLE IF NOT EXISTS consumption (date timestamp NOT NULL, val REAL NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_consumption_date ON consumption (date);
        """,
    )

    @task.virtualenv(
        requirements=["entsoe-py",
                    "pandas==2.3.1",
                    "numpy==2.3.1",
                    "PyYAML==6.0",
                    "requests>=2.31.0",
                    "psycopg2-binary==2.9.6",
                    "pyarrow==21.0.0"], 
        system_site_packages=False,
        queue="cves"
    )
    def get_price(apikey, context):
        from datetime import datetime, timedelta, timezone, tzinfo
        import time
        import pandas as pd
        import requests
        from entsoe import EntsoePandasClient


        """ slow down when doing backfills """
        if context['run_type'] == "backfill":
            time.sleep(15)
            start_date = datetime.strptime(context['ds'], "%Y-%m-%d")
            end_date = start_date + timedelta(days=1)
        else:
            t = datetime.now()
            start_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=-7)
            end_date = datetime(year=t.year,month=t.month,day=t.day, hour=23, minute=00, second=0) + timedelta(days=+1)

        client = EntsoePandasClient(api_key=apikey)
        start = pd.Timestamp(start_date, tz="Europe/Helsinki")
        end = pd.Timestamp(end_date, tz="Europe/Helsinki")
        try:
            ts = client.query_day_ahead_prices("FI",
                                               start=start,
                                               end=end)
            ty = ts.tz_convert("UTC")
            df = pd.DataFrame(data=ty)
            return df
        except requests.HTTPError as ex:
            raise ex

    @task()
    def load_prices(df, hook: PostgresHook):
        for row in df.itertuples():
            data_tuple = (row[0], row[1] / 10)
            execute_query_with_conn_obj("""INSERT INTO price (date, val) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING""", data_tuple, hook)

    context = get_dag_context()
    price_data = get_price(Variable.get("electricity_costs_entsoe_apikey"), context)
    context >> create_electricity_tables >> price_data
    load = load_prices(price_data, hook)
    price_data >> load

entsoe_el()
