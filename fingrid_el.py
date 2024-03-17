import datetime
import pendulum
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.influxdb.hooks.influxdb import InfluxDBHook
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed
from typing import Dict, List


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

    @task.virtualenv(
        requirements=['-r /opt/airflow/dags/pyreqs/fingrid_el.txt '], system_site_packages=False
    )
    def getDatasets(fingrid_apikey):
        import requests, json
        url = f"https://data.fingrid.fi/api/datasets?page=1&pageSize=2000&orderBy=id"
        hdr = {
            'Cache-Control': 'no-cache',
            'x-api-key': f'{fingrid_apikey}',
        }
        response = requests.get(url=url, headers=hdr)
        datasets = json.loads(response.content)['data']
        res = []
        for dataset in datasets:
            tmp = {
                "id": dataset['id'],
                "name": dataset['nameEn'],
            }
            res.append(tmp)
        return res

    @task(task_id="influxdb_task")
    def createBucket(bucket, influxdb_token, influxdb_url, influxdb_org):
        from influxdb_client import InfluxDBClient

        with InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org, verify_ssl=False) as client:
            client.buckets_api().create_bucket(bucket_name=bucket, description="Bucket for fingrid timeseries", org=influxdb_org)
            

    @task.virtualenv(
        requirements=['-r /opt/airflow/dags/pyreqs/fingrid_el.txt '], system_site_packages=False
    )
    def extract(datasets, fingrid_apikey:str, influxdb_token:str, influxdb_url:str, influxdb_org:str, bucket:str):

        import time
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS
        from datetime import datetime, timedelta
        import pandas as pd
        import requests
        import json
        import pandas as pd

        def getPage(id, start, end, page, fingrid_apikey):
            try:
                url = f"https://data.fingrid.fi/api/datasets/{id}/data?startTime={start}&endTime={end}&format=json&page={page}&pageSize=8000&locale=en&sortBy=startTime&sortOrder=asc"
                hdr = {
                    'Cache-Control': 'no-cache',
                    'x-api-key': fingrid_apikey,
                }
                response = requests.get(url=url, headers=hdr)
                time.sleep(20)
                return json.loads(response.content)
            except Exception as e:
                print(e)

        def insert(res, measurement_name, influxdb_url, influxdb_token, influxdb_org, bucket):
            data_frame = pd.DataFrame(data=res['data'])
            data_frame.set_index('startTime', inplace=True)


            with InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org, verify_ssl=False) as client:
                with client.write_api() as write_api:
                    write_api.write(bucket=bucket, record=data_frame,
                                    data_frame_measurement_name=measurement_name)

        t = datetime.now()
        # start date should be the last inserted timeseries measurement and this would have to be forked having for each different starts
        start_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=-2)
        end_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=+1)

        for dataset in datasets:
            start = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            end = end_date.strftime("%Y-%m-%dT%H:%M:%S")
            page = 1
            res = getPage(dataset['id'], start, end, 1, fingrid_apikey )
            time.sleep(20)
            if len(res['data']) > 0:
                insert(res, dataset['name'], influxdb_url=influxdb_url, influxdb_token=influxdb_token, influxdb_org=influxdb_org, bucket=bucket)
                try:
                    lastPage = res['lastPage']
                except:
                    lastPage = page
                while lastPage > page:
                    time.sleep(20)
                    page = page + 1
                    res = getPage(dataset['id'], dataset, start, end, page, fingrid_apikey)
                    insert(res, dataset['name'], influxdb_url=influxdb_url, influxdb_token=influxdb_token, influxdb_org=influxdb_org, bucket=bucket)


    get_datasets = getDatasets(fingrid_apikey=Variable.get("fingrid_apikey"))
    run_influxdb_task = createBucket("fingrid", Variable.get("influxdb_token"), Variable.get("influxdb_url"), Variable.get("influxdb_org"))
    extract_and_load = extract(get_datasets, Variable.get("fingrid_apikey"),Variable.get("influxdb_token"), Variable.get("influxdb_url"), Variable.get("influxdb_org"), "fingrid")

    get_datasets >> run_influxdb_task >> extract_and_load


fingrid_el()
