import datetime
import pendulum
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed

@dag(
    schedule="0 */12 * * *",
    start_date=pendulum.datetime(2024, 1, 4, tz="UTC"),
    catchup=False,
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

    create_electricity_tables = PostgresOperator(
        task_id="create_electricity_tables",
        postgres_conn_id="weather",
        sql="sql/fmi_schema.sql",
    )

    @task.virtualenv(
        requirements=['-r /opt/airflow/dags/pyreqs/fmi_reqs.txt'], system_site_packages=False
    )
    def extract():
        import rasterio
        import fmiopendata
        import datetime as dt
        from fmiopendata.wfs import download_stored_query, get_stored_queries, get_stored_query_descriptions
        loc_list=[]
        obs_list=[]
        end_time = dt.datetime.utcnow() - dt.timedelta(hours=3)
        start_time = end_time - dt.timedelta(hours=24)
        start_time = start_time.isoformat(timespec="seconds") + "Z"
        end_time = end_time.isoformat(timespec="seconds") + "Z"
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
            execute_query_with_conn_obj("""INSERT INTO fcast_loc (fmisid, name, latitude, longitude) VALUES (%s, %s, %s, %s) ON CONFLICT (name) DO NOTHING""", row, hook)
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
                                    temp = EXCLUDED.temp,
                                    hpa = EXCLUDED.hpa,
                                    humidity = EXCLUDED.humidity,
                                    geo_potential_h = EXCLUDED.geo_potential_h,
                                    u_component_wind = EXCLUDED.u_component_wind,
                                    v_component_wind = EXCLUDED.v_component_wind,
                                    rain_mm_hr = EXCLUDED.rain_mm_hr;""", row, hook)

    extract_data = extract()
    create_electricity_tables >> extract_data
    load_obs(extract_data, hook)

fmi_fcast_el()
