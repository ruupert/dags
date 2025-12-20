import datetime
import pendulum

from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.hook import BaseHook
from airflow.models.dagrun import DagRun

@dag(
    schedule="25 1 * * *",
    start_date=pendulum.datetime(2024, 3, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["electricity"],
)
def fingrid_el():
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
    @task()
    def getDatasets(fingrid_apikey, pagesize, wait, ds=None, ti=None, dag_run=None) -> list:
        if dag_run.run_type == "backfill":
            return []

        import time
        import json
        import requests

        nextPage = 1
        result = []
        hdr = {
            'Cache-Control': 'no-cache',
            'x-api-key': f'{fingrid_apikey}',
        }
        while nextPage != None:
            url = f"https://data.fingrid.fi/api/datasets?page={nextPage}&pageSize={pagesize}&orderBy=id"
            response = requests.get(url=url, headers=hdr)
            pagedata = json.loads(response.content)
            nextPage = pagedata['pagination']['nextPage']
            for dataset in pagedata['data']:
                tmp = {
                    "id": dataset['id'],
                    "name": dataset['nameEn'],
                }
                result.append(tmp)
            time.sleep(wait)
        return result

    @task()
    def extract(datasets:list, fingrid_apikey:str, wait:int, pagesize:int, dburi:str, ds=None, ti=None, dag_run=None):
        import time
        import requests
        import json
        from datetime import timedelta, datetime, timezone
        import sqlalchemy
        from sqlalchemy.dialects.postgresql import insert
        from sqlalchemy.sql import text
        import pandas as pd

        """ slow down when doing backfills """
        if dag_run.run_type == "backfill":
            time.sleep(15)

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
                res = pd.concat([res, pd.DataFrame(data=pagedata['data'])], ignore_index=True)
                if "pagination" in pagedata.keys():
                    nextPage = pagedata['pagination']['nextPage']
                    time.sleep(wait)
                else:
                    break

            return res.drop(columns='endTime', errors='ignore').rename(columns={"datasetId":"dataset_id","startTime":"time"})

        start = datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=timezone.utc) - timedelta(days=2)
        end = start + timedelta(days=2)

        engine = sqlalchemy.create_engine(url=dburi.replace("postgres://", "postgresql://", 1))
        with engine.connect() as conn:
            for dataset in datasets:
                statement = text("""INSERT INTO fingrid_links (id, name) VALUES (:id, :name) ON CONFLICT (id) DO NOTHING;""")
                values = {
                    "id": dataset['id'],
                    "name": dataset['name'],
                }
                conn.execute(statement, (values))
        # ids I am interested of are 
        datasetids = [181, 188, 191, 192, 193, 245, 247, 268]
        for datasetid in datasetids:
            tmpdf = getDatasetDf(datasetid, start, end, fingrid_apikey)
            tmpdf.to_sql(   name="fingrid_data", 
                            con=engine,
                            schema="public",
                            if_exists="append",
                            index=False,
                            method=insert_on_conflict_nothing, 
                            chunksize=1000)
        
        """ slow down when doing backfills """
        if dag_run.run_type == "backfill":
            time.sleep(15)


    dburi = BaseHook.get_connection("fingrid_ts").get_uri()
    pagesize = 8000
    wait = 20
    apikey=Variable.get("fingrid_apikey")
    get_datasets = getDatasets(fingrid_apikey=apikey, wait=wait, pagesize=pagesize)
    extract_and_load = extract(datasets=get_datasets, fingrid_apikey=apikey, wait=wait, pagesize=pagesize, dburi=dburi)
    create_fingrid_tables >> get_datasets >> extract_and_load

fingrid_el()
