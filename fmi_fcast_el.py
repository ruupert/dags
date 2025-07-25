import datetime
import pendulum
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator

@dag(
    schedule="0 */6 * * *",
    start_date=pendulum.datetime(2024, 1, 4, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["weather"],
)

def fmi_fcast_el():
    hook = PostgresHook(postgres_conn_id='weather')

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
            CREATE TABLE IF NOT EXISTS fcast_loc (
                                        fcast_loc_id INTEGER,
                                        name TEXT,
                                        latitude REAL,
                                        longitude REAL,
                                        PRIMARY KEY(fcast_loc_id));
            CREATE TABLE IF NOT EXISTS fcast (
                                        id INT GENERATED ALWAYS AS IDENTITY,
                                        fcast_loc_id INT,
                                        date timestamp NOT NULL,
                                        temp REAL,
                                        hpa REAL,
                                        humidity REAL,
                                        geo_potential_h REAL,
                                        u_component_wind REAL,
                                        v_component_wind REAL,
                                        rain_mm_hr REAL,
                                        CONSTRAINT fk_fcast_loc
                                            FOREIGN KEY(fcast_loc_id)
                                                REFERENCES fcast_loc(fcast_loc_id)); 
            CREATE UNIQUE INDEX IF NOT EXISTS idx_fcast_loc_name ON fcast_loc (name);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_fcast_date ON fcast (fcast_loc_id, date);
        """,
    )

    @task.virtualenv(
        requirements=['Numpy==1.26.4',
                    'rasterio==1.3.9',
                    'fmiopendata'],
        system_site_packages=False
    )
    def extract():
        import rasterio
        import fmiopendata
        import datetime as dt
        from fmiopendata.wfs import download_stored_query, get_stored_queries, get_stored_query_descriptions
        loc_list=[]
        obs_list=[]
        end_time = (dt.datetime.utcnow() + dt.timedelta(hours=72)).isoformat(timespec="seconds") + "Z"
        start_time = (dt.datetime.utcnow() - dt.timedelta(hours=6)).isoformat(timespec="seconds") + "Z"
        obs = download_stored_query("ecmwf::forecast::surface::cities::multipointcoverage",
                                    args=["bbox=21,60,35,75",
                                        "starttime=" + start_time,
                                        "endtime=" + end_time,                                
                                        "timestep=60"])
        for key in obs.location_metadata.keys():
            loc_list.append((obs.location_metadata[key]['fmisid'], 
                            key,
                            obs.location_metadata[key]['latitude'],
                            obs.location_metadata[key]['longitude']))
        locs = {}
        for item in obs.location_metadata:
            locs.update({item : obs.location_metadata[item]['fmisid'] })
        for key in obs.data:
            for loc in locs.keys():
                obs_list.append((
                            locs[loc],
                            key,
                            obs.data[key][loc]['Air temperature']['value'],
                            obs.data[key][loc]['Air pressure']['value'],
                            obs.data[key][loc]['Humidity']['value'],
                            obs.data[key][loc]['Geopotential height']['value'],
                            obs.data[key][loc]['U-component of wind vector']['value'],
                            obs.data[key][loc]['V-component of wind']['value'],
                            obs.data[key][loc]['Precipitation amount 1 hour']['value']))
        return {'locs': loc_list, 'obs': obs_list }

    @task()
    def load_obs(tuples_lists, hook: PostgresHook):
        for row in tuples_lists['locs']:
            execute_query_with_conn_obj("""INSERT INTO fcast_loc (fcast_loc_id, name, latitude, longitude) VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING""", row, hook)
        for row in tuples_lists['obs']:
            execute_query_with_conn_obj("""INSERT INTO fcast (
                                    fcast_loc_id, 
                                    date, 
                                    temp, 
                                    hpa, 
                                    humidity, 
                                    geo_potential_h, 
                                    u_component_wind, 
                                    v_component_wind, 
                                    rain_mm_hr) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (fcast_loc_id, date) DO UPDATE SET 
                                    temp = COALESCE(EXCLUDED.temp, fcast.temp),
                                    hpa = COALESCE(EXCLUDED.hpa, fcast.hpa),
                                    humidity = COALESCE(EXCLUDED.humidity, fcast.humidity),
                                    geo_potential_h = COALESCE(EXCLUDED.geo_potential_h,fcast.geo_potential_h),
                                    u_component_wind = COALESCE(EXCLUDED.u_component_wind, fcast.u_component_wind),
                                    v_component_wind = COALESCE(EXCLUDED.v_component_wind, fcast.v_component_wind),
                                    rain_mm_hr = COALESCE(EXCLUDED.rain_mm_hr, fcast.rain_mm_hr);""", row, hook)

    extract_data = extract()
    create_electricity_tables >> extract_data
    load_obs(extract_data, hook)

fmi_fcast_el()
