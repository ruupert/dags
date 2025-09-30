import datetime
import pendulum
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator

@dag(
    schedule="5 */8 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["electricity"],
)

def helen_el():
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
        requirements=["pandas==1.5.3",
                "Numpy==1.26.4",
                "PyYAML==6.0",
                "requests>=2.31.0",
                "psycopg2-binary==2.9.6",
                "pyroscope-io==0.8.5",
                "helen_electricity_usage @ git+https://github.com/ruupert/python-helen-electricity-usage@master#egg=helen_electricity_usage"], 
        system_site_packages=False
    )
    def extract(username, password, delivery_site):
        import datetime
        import math
        import os
        import time
        from datetime import datetime, timedelta, timezone
        from pathlib import Path
        import pandas
        import helen_electricity_usage


        t = datetime.now()
        start_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0, tzinfo=timezone.utc) + timedelta(days=-7)
        end_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0, tzinfo=timezone.utc)

        helen = helen_electricity_usage.Helen(username, password, delivery_site)
        helen.login()
        data = helen.get_electricity(start_date, end_date)
        
        data = data['intervals']['electricity'][0]
        hours = len(data['measurements'])-1
        idx = pandas.date_range(start=datetime.strptime( data['start'], "%Y-%m-%dT%H:%M:%SZ"), end=(datetime.strptime( data['start'], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=hours)), freq='h')

        pa = pandas.array(data['measurements'])

        df = pandas.DataFrame(data=pa, index=idx)
        return df

    @task()
    def load_consumption(df, hook: PostgresHook):
        for row in df.itertuples():
            if row[1]['status'] == 'valid':
                data_tuple = (row[0].tz_localize('utc').tz_convert("Europe/Helsinki"), row[1]['value'])
                execute_query_with_conn_obj("""INSERT INTO consumption (date, val) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING""", data_tuple, hook)

    consumption_data = extract(Variable.get("electricity_costs_helen_username"),
                         Variable.get("electricity_costs_helen_password"),
                         Variable.get("electricity_costs_delivery_site"))
    create_electricity_tables >> consumption_data
    load_consumption(consumption_data, hook)

helen_el()
