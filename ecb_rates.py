# import datetime
# import pendulum
# from airflow.models.dag import DAG
# from airflow.models import Variable
# from airflow.sdk import dag, task
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.standard.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator
# from airflow.sdk.bases.hook import BaseHook
# from airflow.providers.standard.operators.empty import EmptyOperator
# from airflow.utils.dag_parsing_context import get_parsing_context
# 
# current_dag_id = get_parsing_context().dag_id
# 
# currencies = ["USD","GBP","CHF","SEK","JPY","CNY","NOK","AUD","INR","ZAR","TRY","RUB","ILS"]
# minutes=5
# hour=18
# for c in currencies:
#     dag_id = f"ecb_get_rate_{c}"
#     if current_dag_id is not None and current_dag_id != dag_id:
#         continue  # skip generation of non-selected DAG
#     minutes += 5
#     if minutes >= 60:
#         minutes = minutes - 60
#         hour += 1
# 
#     with DAG(
#         schedule=f"{minutes} {hour} * * *",
#         start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
#         catchup=False,
#         dag_id=dag_id,
#         max_active_runs=1,
#         default_args={
#             "depends_on_past": False,
#             "retries": 3,
#             "retry_delay": datetime.timedelta(minutes=45),
#         },
#         tags=["finance"]
#     ):
#         hook = PostgresHook(postgres_conn_id='stocks_ts')
# 
#         def execute_query_with_conn_obj(query, datatuple, hook):
#             conn = hook.get_conn()
#             cur = conn.cursor()
#             cur.execute(query, datatuple)
#             conn.commit()
# 
#         end = EmptyOperator(task_id="end")
#         create_ecb_tables = SQLExecuteQueryOperator(
#             task_id="create_ecb_rates_table",
#             conn_id="stocks_ts",
#             sql="""
#                 CREATE TABLE IF NOT EXISTS ecb_rate_eur ( 
#                         time timestamp not null, 
#                         currency text, 
#                         rate FLOAT 
#                 );
#                 CREATE EXTENSION IF NOT EXISTS timescaledb;
#                 SELECT create_hypertable('ecb_rate_eur', by_range('time'), if_not_exists => TRUE);
#                 CREATE UNIQUE INDEX IF NOT EXISTS idx_ecb_eur_time_curr on ecb_rate_eur(currency, time);
#             """,
#         )
#         @task.virtualenv(
#             requirements=['ecbdata', 'pandas==2.2.0', 'PyYAML==6.0', 'requests==2.31.0', 'psycopg2-binary==2.9.6', 'SQLAlchemy==2.0.25'],
#             task_id="get_rate",
#             system_site_packages=False
#         )
#         def get_rates(c, dburi):
#             import sqlalchemy
#             from ecbdata import ecbdata
#             from sqlalchemy.dialects.postgresql import insert
#             from sqlalchemy.sql import text
#             from datetime import datetime, timedelta
# 
#             def insert_on_conflict_nothing(table, conn, keys, data_iter):
#                 data = [dict(zip(keys, row)) for row in data_iter]
#                 insert_statement = insert(table.table).values(data)
#                 upsert_statement = insert_statement.on_conflict_do_nothing()
#                 conn.execute(upsert_statement)
# 
#             t = datetime.now()
#             start_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=-7)
#             end_date = datetime(year=t.year,month=t.month,day=t.day, hour=23, minute=00, second=0)
# 
#             try:
#                 df = ecbdata.get_series(f'EXR.D.{c}.EUR.SP00.A', start=start_date.strftime('%Y-%m'), end=end_date.strftime('%Y-%m'))
#             except:
#                 return 0
# 
#             ddf = df[df.OBS_STATUS == 'A']
#             ddf = ddf[['TIME_PERIOD','CURRENCY','OBS_VALUE']]
#             ddf = ddf.rename(columns={"TIME_PERIOD": "time", "CURRENCY": "currency", "OBS_VALUE":"rate"})
# 
#             engine = sqlalchemy.create_engine(url=dburi.replace("postgres://", "postgresql://", 1))
# 
#             ddf.to_sql(     name="ecb_rate_eur", 
#                             con=engine,
#                             schema="public",
#                             if_exists="append",
#                             index=False,
#                             method=insert_on_conflict_nothing, 
#                             chunksize=1000)
# 
#         get_rate = get_rates(c, BaseHook.get_connection("stocks_ts").get_uri())
#         create_ecb_tables >> get_rate >> end
