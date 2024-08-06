import datetime
import pendulum

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook

@dag(
    schedule="25 13 * * *",
    start_date=pendulum.datetime(2024, 3, 16, tz="UTC"),
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["electricity"],
)
def fingrid_el():
    create_fingrid_tables = PostgresOperator(
        task_id="create_fingrid_tables",
        postgres_conn_id="fingrid_ts",
        sql="sql/fingrid_schema.sql",
    )
    @task.virtualenv(
        requirements=['pandas', 'requests'], system_site_packages=False
    )
    def getDatasets(fingrid_apikey, pagesize, wait) -> list:
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
    @task.virtualenv(
        requirements=['pandas==1.5.3', 'Numpy==1.26.4', 'PyYAML==6.0', 'requests==2.31.0', 'psycopg2-binary==2.9.6', 'SQLAlchemy==1.4.17'], system_site_packages=False
    )
    def extract(datasets:list, fingrid_apikey:str, wait:int, pagesize:int, dburi:str):
        import time
        import requests
        import json
        from datetime import timedelta, datetime
        import psycopg2
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
        engine = sqlalchemy.create_engine(url=dburi)
        with engine.connect() as conn:
            for dataset in datasets:
                statement = text("""INSERT INTO fingrid_links (id, name) VALUES (:id, :name) ON CONFLICT (id) DO NOTHING;""")
                conn.execute(statement, (dataset['id'], dataset['name']))
        for dataset in datasets:
            tmpdf = getDatasetDf(dataset['id'], start, end, fingrid_apikey)
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
    get_datasets = getDatasets(fingrid_apikey=apikey, wait=wait, pagesize=pagesize)
    extract_and_load = extract(datasets=get_datasets, fingrid_apikey=apikey, wait=wait, pagesize=pagesize, dburi=dburi)
    create_fingrid_tables >> get_datasets >> extract_and_load

fingrid_el()
