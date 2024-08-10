import datetime
import pendulum

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook

@dag(
    schedule="25 09 * * *",
    start_date=pendulum.datetime(2024, 8, 10, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["finance"],
)
def stocks_el():
    create_stocks_tables = PostgresOperator(
        task_id="create_stocks_tables",
        postgres_conn_id="stocks_ts",
        sql="sql/stocks_schema.sql",
    )

    @task.virtualenv(
        requirements=['pandas==2.2.2', "psycopg2_binary==2.9.9", "SQLAlchemy==2.0.31", "yfinance==0.2.41", "scipy"], system_site_packages=False
    )
    def getData(tickers, dburi):
        import yfinance as yf
        import pandas
        import psycopg2
        import datetime
        from sqlalchemy.dialects.postgresql import insert
        from sqlalchemy.sql import text
        import sqlalchemy

        def insert_on_conflict_nothing(table, conn, keys, data_iter):
            data = [dict(zip(keys, row)) for row in data_iter]
            insert_statement = insert(table.table).values(data)
            upsert_statement = insert_statement.on_conflict_do_nothing()
            conn.execute(upsert_statement)
        
        def findMinDate(records:list):
            t = datetime.datetime.now()
            for r in records:
                if r[0] < t:
                    t = r[0]
            return t
        
        engine = sqlalchemy.create_engine(url=dburi.replace("postgres://", "postgresql://", 1))
        conn = engine.connect()
        stmt = text("select tmp.time, tmp.ticker from (SELECT DISTINCT ON (ticker) * FROM stock_data WHERE time > now() - INTERVAL '300 days' and ticker in :tickers ORDER BY ticker, time DESC) as tmp;")
        vals = { "tickers": tuple(tickers) }
        res = conn.execute(stmt, (vals)).fetchall()
        if len(res) < len(tickers):
            df = yf.download(tickers, period='max', group_by='Ticker', repair=True)
        else:
            df = yf.download(tickers, start=findMinDate(res), group_by='Ticker', repair=True)
        tmpdf = df.stack(level=0,future_stack=True).rename_axis(['Date', 'Ticker']).reset_index(level=1).dropna().reset_index()
        tmpdf = tmpdf.rename(columns={
            "Date": "time",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adjclose",
            "Volume": "volume",
            "Repaired?": "repaired",
            "Ticker": "ticker",
        })
        tmpdf.to_sql('stock_data', 
            conn,schema='public',
            if_exists='append',
            index=False,
            method=insert_on_conflict_nothing)
        conn.close()

    dburi = BaseHook.get_connection("stocks_ts").get_uri()
    tickers = ["CCL", "CA.PA", "NDA-FI.HE", "ULVR.L", "ANORA.HE", "LHA.DE", "FORTUM.HE", "KESKOB.HE", "KNEBV.HE", "VIK1V.HE", "GSK.L", "TELIA.ST", "HSBA.L", "NOVN.SW", "SDZ.SW"]
    get_data = getData(tickers=tickers, dburi=dburi)
    create_stocks_tables >> get_data

stocks_el()
