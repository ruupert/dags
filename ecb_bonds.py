import datetime
import pendulum
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.dag_parsing_context import get_parsing_context

current_dag_id = get_parsing_context().dag_id

bonds = ["SR_10Y","SR_1Y", "SR_5Y", "SR_3M", "SR_2Y", "SR_30Y", "SR_20Y", 
        "SR_7Y", "SR_6M", "SR_3Y", "SR_15Y", "SR_6Y", "SR_12Y", "SR_14Y", 
        "SR_18Y", "SR_11Y", "SR_25Y", "SR_17Y", "SR_29Y", "SR_28Y", "SR_19Y"]

minutes=5
hour=3
for b in bonds:
    dag_id = f"ecb_aaa_bond_{b}"
    if current_dag_id is not None and current_dag_id != dag_id:
        continue
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
        tags=["finance"]
    ):
        hook = PostgresHook(postgres_conn_id='stocks_ts')

        def execute_query_with_conn_obj(query, datatuple, hook):
            conn = hook.get_conn()
            cur = conn.cursor()
            cur.execute(query, datatuple)
            conn.commit()

        end = EmptyOperator(task_id="end")
        create_ecb_tables = PostgresOperator(
            task_id="create_ecb_bonds_table",
            postgres_conn_id="stocks_ts",
            sql="sql/ecb_bonds_schema.sql",
        )
        @task.virtualenv(
            requirements=['ecbdata', 'pandas==2.2.0', 'PyYAML==6.0', 'requests==2.31.0', 'psycopg2-binary==2.9.6', 'SQLAlchemy==2.0.25'],
            task_id="get_bond",
            system_site_packages=False
        )
        def get_bonds(b, dburi):
            import sqlalchemy
            from ecbdata import ecbdata
            from sqlalchemy.dialects.postgresql import insert
            from sqlalchemy.sql import text
            from datetime import datetime, timedelta

            def insert_on_conflict_nothing(table, conn, keys, data_iter):
                data = [dict(zip(keys, row)) for row in data_iter]
                insert_statement = insert(table.table).values(data)
                upsert_statement = insert_statement.on_conflict_do_nothing()
                conn.execute(upsert_statement)

            t = datetime.now()
            start_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0) + timedelta(days=-7)
            end_date = datetime(year=t.year,month=t.month,day=t.day, hour=23, minute=00, second=0)

            try:
                df = ecbdata.get_series(f'YC.B.U2.EUR.4F.G_N_A.SV_C_YM.{b}', start=start_date.strftime('%Y-%m'), end=end_date.strftime('%Y-%m'))
            except:
                return 0

            ddf = df[df.OBS_STATUS == 'A']
            ddf = ddf[['TIME_PERIOD','DATA_TYPE_FM','OBS_VALUE']]
            ddf = ddf.rename(columns={"TIME_PERIOD": "time", "DATA_TYPE_FM": "bond", "OBS_VALUE":"yield"})

            engine = sqlalchemy.create_engine(url=dburi.replace("postgres://", "postgresql://", 1))

            ddf.to_sql(     name="ecb_bonds", 
                            con=engine,
                            schema="public",
                            if_exists="append",
                            index=False,
                            method=insert_on_conflict_nothing, 
                            chunksize=1000)

        get_bond = get_bonds(b, BaseHook.get_connection("stocks_ts").get_uri())
        create_ecb_tables >> get_bond >> end
