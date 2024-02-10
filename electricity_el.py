import datetime
import pendulum
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed

@dag(
    schedule="25 13 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["electricity"],
)

def electricity_el():
    hook = PostgresHook(postgres_conn_id='electricity')

    def execute_query_with_conn_obj(query, datatuple, hook):
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(query, datatuple)
        conn.commit()


    create_electricity_tables = PostgresOperator(
        task_id="create_electricity_tables",
        postgres_conn_id="electricity",
        sql="sql/electricity_schema.sql",
    )

    @task.virtualenv(
        requirements=['-r /opt/airflow/dags/pyreqs/electricity_requirements.txt '], system_site_packages=False
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
        idx = pandas.date_range(start=datetime.strptime( data['start'], "%Y-%m-%dT%H:%M:%SZ"), end=(datetime.strptime( data['stop'], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=-1)), freq='H')
        pa = pandas.array(data['measurements'])

        df = pandas.DataFrame(data=pa, index=idx)
        return df

    @task.virtualenv(
        requirements=['-r /opt/airflow/dags/pyreqs/electricity_requirements.txt '], system_site_packages=False
    )
    def get_price(apikey):
        from datetime import datetime, timedelta, timezone, tzinfo

        import pandas as pd
        import requests
        from entsoe import EntsoePandasClient
        t = datetime.now()
        start_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=-7)
        end_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=+2)
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
    def load_consumption(df, hook: PostgresHook):
        for row in df.itertuples():
            if row[1]['status'] == 'valid':
                data_tuple = (row[0].tz_localize('utc').tz_convert("Europe/Helsinki"), row[1]['value'])
                execute_query_with_conn_obj("""INSERT INTO consumption (date, val) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING""", data_tuple, hook)

    @task()
    def load_prices(df, hook: PostgresHook):
        for row in df.itertuples():
            data_tuple = (row[0], row[1] / 10)
            execute_query_with_conn_obj("""INSERT INTO price (date, val) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING""", data_tuple, hook)


    consumption_data = extract(Variable.get("electricity_costs_helen_username"),
                         Variable.get("electricity_costs_helen_password"),
                         Variable.get("electricity_costs_delivery_site"))
    price_data = get_price(Variable.get("electricity_costs_entsoe_apikey"))
    create_electricity_tables >> price_data
    create_electricity_tables >> consumption_data
    load_consumption(consumption_data, hook)
    load_prices(price_data, hook)

electricity_el()
