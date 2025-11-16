import datetime
import pendulum
from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator

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
        system_site_packages=False
    )
    def get_price(apikey):
        from datetime import datetime, timedelta, timezone, tzinfo

        import pandas as pd
        import requests
        from entsoe import EntsoePandasClient
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


    price_data = get_price(Variable.get("electricity_costs_entsoe_apikey"))
    create_electricity_tables >> price_data
    load_prices(price_data, hook)

entsoe_el()
