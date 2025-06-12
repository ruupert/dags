import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.base import BaseHook
from airflow.utils.dag_parsing_context import get_parsing_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import io
import json

current_dag_id = get_parsing_context().dag_id

s3 = S3Hook('assets', transfer_config_args={
    'use_threads': False
    })
file_object = io.BytesIO()
s3.get_key("fingrid_api_datasets.json", bucket_name="assets").download_fileobj(file_object)
file_object.seek(0)
datasets = json.loads(file_object.read())

minutes=5
hour=1


for dataset in datasets:
    ds_name = str(dataset['name'])
    ds_stripped = ''.join(e for e in ds_name if e.isalnum()).encode('ascii', 'ignore').decode('ascii')
    dag_id = f"fingrid_dataset_get_{ds_stripped}"
    if current_dag_id is not None and current_dag_id != dag_id:
        continue  # skip generation of non-selected DAG
    minutes += 5
    if minutes >= 60:
        minutes = minutes - 60
        hour += 1

    with DAG(
        schedule=f"{minutes} {hour} * * *",
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        dag_id=dag_id,
        max_active_runs=1,
        default_args={
            "depends_on_past": False,
            "retries": 3,
            "retry_delay": datetime.timedelta(minutes=45),
        },
        tags=["electricity"]
    ):
        
        create_fingrid_tables = SQLExecuteQueryOperator(
            task_id="create_fingrid_tables",
            conn_id="fingrid_ts",
            sql="""
                CREATE TABLE IF NOT EXISTS fingrid_links (id BIGSERIAL PRIMARY KEY, name text);
                CREATE TABLE IF NOT EXISTS fingrid_links (id INTEGER, name text);
                CREATE TABLE IF NOT EXISTS fingrid_data (time TIMESTAMP not null, dataset_id INTEGER, value FLOAT);
                CREATE EXTENSION IF NOT EXISTS timescaledb;
                SELECT create_hypertable('fingrid_data', by_range('time'), if_not_exists => TRUE);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_fingrid_links_id ON fingrid_links (id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_fingrid_data_time_dataset_id ON fingrid_data (time, dataset_id);
            """,
        )

        @task
        def extract(dss:int, fingrid_apikey:str, wait:int, pagesize:int, dburi:str):
            import time
            import requests
            import json
            from datetime import timedelta, datetime
            import sqlalchemy
            from sqlalchemy.dialects.postgresql import insert
            from sqlalchemy.sql import text
            import pandas as pd
            def insert_on_conflict_nothing(table, conn, keys, data_iter):
                data = [dict(zip(keys, row)) for row in data_iter]
                insert_statement = insert(table.table).values(data)
                upsert_statement = insert_statement.on_conflict_do_nothing()
                conn.execute(upsert_statement)
            def getDatasetDf(id, start, end, apikey) -> pd.DataFrame:
                nextPage = 1
                res = pd.DataFrame()
                hdr = {
                    'Cache-Control': 'no-cache',
                    'x-api-key': f'{apikey}',
                }
                while nextPage != None:
                    url = f"https://data.fingrid.fi/api/datasets/{id}/data?startTime={start}&endTime={end}&format=json&page={nextPage}&pageSize={pagesize}&locale=en&sortBy=startTime&sortOrder=asc"
                    response = requests.get(url=url, headers=hdr)
                    pagedata = json.loads(response.content)
                    nextPage = pagedata['pagination']['nextPage']
                    res = pd.concat([res, pd.DataFrame(data=pagedata['data'])], ignore_index=True)
                    time.sleep(wait)
                return res.drop(columns='endTime', errors='ignore').rename(columns={"datasetId":"dataset_id","startTime":"time"})
            t = datetime.now()
            start = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=-2)
            end = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=+1)
            engine = sqlalchemy.create_engine(url=dburi.replace("postgres://", "postgresql://", 1))
            with engine.connect() as conn:
                statement = text("""INSERT INTO fingrid_links (id, name) VALUES (:id, :name) ON CONFLICT (id) DO NOTHING;""")
                values = {
                    "id": dss['id'],
                    "name": dss['name'],
                }
                conn.execute(statement, (values))
            tmpdf = getDatasetDf(dss['id'], start, end, fingrid_apikey)
            tmpdf.to_sql(   name="fingrid_data", 
                            con=engine,
                            schema="public",
                            if_exists="append",
                            index=False,
                            method=insert_on_conflict_nothing, 
                            chunksize=1000)

        dburi = BaseHook.get_connection("fingrid_ts").get_uri()
        pagesize = 8000
        wait = 20
        apikey=Variable.get("fingrid_apikey")
        extract_and_load = extract(dss=dataset, fingrid_apikey=apikey, wait=wait, pagesize=pagesize, dburi=dburi)
        create_fingrid_tables >> extract_and_load