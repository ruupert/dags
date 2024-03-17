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

def fingrid_el(fingrid_apikey:str):

    @task.virtualenv(
        requirements=['-r /opt/airflow/dags/pyreqs/fingrid_el.txt '], system_site_packages=False
    )
    def getDatasets():
        import urllib, json
        import pandas as pd
        urllib.disable_warnings()
        url = f"https://data.fingrid.fi/api/datasets?page=1&pageSize=2000&orderBy=id"
        hdr = {
            'Cache-Control': 'no-cache',
            'x-api-key': fingrid_apikey,
        }
        req = urllib.request.Request(url, headers=hdr)
        req.get_method = lambda: 'GET'
        response = urllib.request.urlopen(req)
        datasets = json.loads(response.read())['data']
        res = []
        for dataset in datasets:
            tmp = {
                "id": dataset['id'],
                "name": dataset['nameEn'],
            }
            res.append(tmp)
        return res

    @task(task_id="influxdb_task")
    def createBucket():
        influxdb_hook = InfluxDBHook(conn_id="influxdb")
        influxdb_hook.create_bucket("fingrid", "Bucket for fingrid timeseries", influxdb_hook.org_name)
            

    @task.virtualenv(
        requirements=['-r /opt/airflow/dags/pyreqs/fingrid_el.txt '], system_site_packages=False
    )
    def extract(datasets, fingrid_apikey:str, influxdb_token:str, influxdb_url:str, influxdb_org:str, bucket:str):

        import influxdb_client, os, time
        from influxdb_client import InfluxDBClient, Point, WritePrecision
        from influxdb_client.client.write_api import SYNCHRONOUS
        from datetime import datetime, timedelta
        import pandas as pd

        import urllib
        import json

        import pandas as pd

        urllib.disable_warnings()


        def getPage(id, start, end, page, fingrid_apikey):
            try:
                url = f"https://data.fingrid.fi/api/datasets/{id}/data?startTime={start}&endTime={end}&format=json&page={page}&pageSize=8000&locale=en&sortBy=startTime&sortOrder=asc"
                hdr = {
                    'Cache-Control': 'no-cache',
                    'x-api-key': fingrid_apikey,
                }
                req = urllib.request.Request(url, headers=hdr)
                req.get_method = lambda: 'GET'
                response = urllib.request.urlopen(req)
                return json.loads(response.read())
            except Exception as e:
                print(e)

        def insert(res, influxdb_url, influxdb_token, influxdb_org, bucket):
    
            data_frame = pd.DataFrame(data=res['data'])
            data_frame.set_index('startTime', inplace=True)


            with InfluxDBClient(url=influxdb_url, token=influxdb_token, org=influxdb_org, verify_ssl=False) as client:
                with client.write_api() as write_api:
                    write_api.write(bucket=bucket, record=data_frame,
                                    data_frame_measurement_name=res['name'])

        t = datetime.now()
        # start date should be the last inserted timeseries measurement and this would have to be forked having for each different starts
        start_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=-2)
        end_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=+1)

        for dataset in datasets:
            start = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            end = end_date.strftime("%Y-%m-%dT%H:%M:%S")
            page = 1
            res = getPage(dataset['id'], start, end, 1, fingrid_apikey )
            insert(res, influxdb_url=influxdb_url, influxdb_token=influxdb_token, influxdb_org=influxdb_org, bucket=bucket)
            while res['lastPage'] > page:
                time.sleep(10)
                page = page + 1
                res = getPage(dataset['id'], start, end, page, fingrid_apikey)
                insert(res, influxdb_url=influxdb_url, influxdb_token=influxdb_token, influxdb_org=influxdb_org, bucket=bucket)


    get_datasets = getDatasets()
    run_influxdb_task = createBucket(bucket="fingrid")
    extract_and_load = extract(datasets=get_datasets, fingrid_apikey=Variable.get("fingrid_apikey"),influxdb_url=Variable.get("influxdb_url"), influxdb_token=Variable.get("influxdb_token", influxdb_org=Variable.get("influxdb_org"), bucket="fingrid"))

    get_datasets >> run_influxdb_task >> extract_and_load


fingrid_el()
