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
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["weather"],
)

def fmi_el():
    hook = PostgresHook(postgres_conn_id='weather')

    @task(
        queue="cves",
        task_id="dag_context"
    )
    def get_dag_context(ti=None, dag_run=None, ds=None):
        logger.info(f"backfill date: {ds}, run_type: {dag_run.run_type}")
        return {"run_type": dag_run.run_type, "ds": ds}

    # expensive, but works as a hook, todo: psycopg2 extras batch load
    def execute_query_with_conn_obj(query, datatuple, hook):
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(query, datatuple)
        conn.commit()

    create_electricity_tables = SQLExecuteQueryOperator(
        task_id="create_electricity_tables",
        conn_id="weather",
        sql="""
            CREATE TABLE IF NOT EXISTS loc (
                                        loc_id INT,
                                        name TEXT,
                                        latitude REAL,
                                        longitude REAL,
                                        PRIMARY KEY(loc_id));
            CREATE TABLE IF NOT EXISTS obs (
                                        id INT GENERATED ALWAYS AS IDENTITY,
                                        loc_id INT,
                                        date timestamp NOT NULL,
                                        temp_c REAL,
                                        wind_speed_ms REAL,
                                        wind_gust_ms REAL,
                                        wind_direction_deg REAL,
                                        humidity REAL,
                                        dew_point_temp_c REAL,
                                        precipitation_mm REAL,
                                        precipitation_mmh REAL,
                                        snow_depth_cm REAL,
                                        pressure_hpa REAL,
                                        horiz_vis_m REAL,
                                        cloud_amount REAL,
                                        present_weather REAL,
                                        CONSTRAINT fk_loc
                                            FOREIGN KEY(loc_id)
                                                REFERENCES loc(loc_id));
            CREATE UNIQUE INDEX IF NOT EXISTS idx_loc_name ON loc (name);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_date ON obs (loc_id, date);
        """,
    )

    @task.virtualenv(
        requirements=['Numpy==1.26.4',
                    'rasterio==1.3.9',
                    'fmiopendata'],
        system_site_packages=False,
        task_id="extract_fmi_obs"
    )
    def extract(context):
        import json
        import rasterio
        import fmiopendata
        import datetime as dt
        import logging
        import time
        from fmiopendata.wfs import download_stored_query, get_stored_queries, get_stored_query_descriptions

        logger = logging.getLogger(__name__)

        """ slow down when doing backfills """
        if context['run_type'] == "backfill":
            time.sleep(15)
            start_time = dt.datetime.strptime(context['ds'], "%Y-%m-%d")
            end_time = start_time + dt.timedelta(days=1)
            logger.info(f"start: {start_time} end: {end_time}")
        else:
            t = dt.datetime.now()
            start_time = dt.datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) - dt.timedelta(hours=72)
            end_time = dt.datetime(year=t.year,month=t.month,day=t.day, hour=23, minute=0, second=0) + dt.timedelta(hours=10)

        loc_list=[]
        obs_list=[]
        start_time = start_time.isoformat(timespec="seconds") + "Z"
        end_time = end_time.isoformat(timespec="seconds") + "Z"
        obs = download_stored_query("fmi::observations::weather::multipointcoverage",
                                    args=["bbox=21,60,35,75",
                                        "starttime=" + start_time,
                                        "endtime=" + end_time,
                                        "timestep=60"])
        for key in obs.location_metadata.keys():
            loc_list.append([obs.location_metadata[key]['fmisid'], 
                            key,
                            obs.location_metadata[key]['latitude'],
                            obs.location_metadata[key]['longitude']])
        locs = {}
        for item in obs.location_metadata:
            locs.update({item : obs.location_metadata[item]['fmisid'] })
        for key in obs.data:
            for loc in locs.keys():
                obs_list.append([
                            locs[loc],
                            key,
                            obs.data[key][loc]['Air temperature']['value'],
                            obs.data[key][loc]['Wind speed']['value'],
                            obs.data[key][loc]['Gust speed']['value'],
                            obs.data[key][loc]['Wind direction']['value'],
                            obs.data[key][loc]['Relative humidity']['value'],
                            obs.data[key][loc]['Dew-point temperature']['value'],
                            obs.data[key][loc]['Precipitation amount']['value'],
                            obs.data[key][loc]['Precipitation intensity']['value'],
                            obs.data[key][loc]['Snow depth']['value'],
                            obs.data[key][loc]['Pressure (msl)']['value'],
                            obs.data[key][loc]['Horizontal visibility']['value'],
                            obs.data[key][loc]['Cloud amount']['value'],
                            obs.data[key][loc]['Present weather (auto)']['value']])
        return json.dumps({'locs': loc_list, 'obs': obs_list }, default=str)

    @task()
    def load_obs(input, hook: PostgresHook):
        import json
        tuples_lists = json.loads(input)
        for row in tuples_lists['locs']:
            try:
                execute_query_with_conn_obj("""INSERT INTO loc (loc_id, name, latitude, longitude) VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING""", tuple(row), hook)
            except Exception as e:
                print(e)
        for row in tuples_lists['obs']:
            execute_query_with_conn_obj("""INSERT INTO obs (   loc_id,
                                                date,
                                                temp_c,
                                                wind_speed_ms,
                                                wind_gust_ms,
                                                wind_direction_deg,
                                                humidity,
                                                dew_point_temp_c,
                                                precipitation_mm,
                                                precipitation_mmh,
                                                snow_depth_cm,
                                                pressure_hpa,
                                                horiz_vis_m,
                                                cloud_amount,
                                                present_weather)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (loc_id, date) DO NOTHING""", tuple(row), hook)
    context = get_dag_context()

    extract_fmi_obs = extract(context)
    context >> create_electricity_tables >> extract_fmi_obs
    load_obs(extract_fmi_obs, hook)

fmi_el()
