import datetime
import pendulum

from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from rabbitmq_provider.sensors.rabbitmq import RabbitMQSensor

@task
def pull_bad_tickers(ti=None):
    return ti.xcom_pull(key="return_value", task_ids="rabbitsense")

@task
def pull_rabbitsense_batch(ti=None):
    return ti.xcom_pull(key="return_value", task_ids="rabbitsense")

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

    rabbitmq_sensor = RabbitMQSensor(
        task_id="rabbitsense",
        queue_name="inbound",
        rabbitmq_conn_id="rabbitmq",
    )

    create_stocks_tables = PostgresOperator(
        task_id="create_stocks_tables",
        postgres_conn_id="stocks_ts",
        sql="sql/stocks_schema.sql",
    )

    @task.virtualenv(
            requirements=['requests'], system_site_packages=False
    )
    def get_proxies() -> dict:
        import requests
        import json
        return json.loads(requests.get("https://api.proxyscrape.com/v3/free-proxy-list/get?request=displayproxies&country=nl,de,fr,gb,ch,se,ca,it,sa,be,sk,no,gr,at,mx,lt,dk,ie,lv,pt,mt,cy,ee,gi,za,pl,es,rs,bg,cz,ro,si,fi,hr&protocol=http&proxy_format=protocolipport&format=json&anonymity=Anonymous,Elite,Transparent&timeout=6813").content)['proxies']

    @task.virtualenv(
        task_id="fetch_data",
        requirements=['pandas==2.2.2', "psycopg2_binary==2.9.9", "SQLAlchemy==2.0.31", "yfinance==0.2.41", "scipy"], 
        system_site_packages=False,
        do_xcom_push=True,
    )
    def getData(tickers, dburi, proxies):
        import yfinance as yf
        import pandas
        import psycopg2
        import datetime
        import random
        from sqlalchemy.dialects.postgresql import insert
        from sqlalchemy.sql import text
        import sqlalchemy
        import json

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
        
        def findBadTickers(tickers:list, result:list) -> list:
            return  list(set(tickers) - set(result))
        
        #LOL: ... because proxies might not werk or return shit, so try 3 times and give up or something..
        # unsure if this catches the exceptions from children
        def yfDlMax(tickers, period, group_by, repair, proxies):
            tries = 0
            while tries < 3:
                try:
                    df = yf.download(tickers=tickers, period=period, group_by=group_by, repair=repair, proxy=random.choice(proxies)['proxy'])
                    break
                except:
                    tries += 1
                finally:
                    return df
        def yfDlStart(tickers, start, group_by, repair, proxies):
            tries = 0
            while tries < 3:
                try:
                    df = yf.download(tickers=tickers, start=start, group_by=group_by, repair=repair, proxy=random.choice(proxies)['proxy'])
                    break
                except:
                    tries += 1
                finally:
                    return df

        tickers = json.loads(tickers)
        engine = sqlalchemy.create_engine(url=dburi.replace("postgres://", "postgresql://", 1))
        conn = engine.connect()
        stmt = text("select tmp.time, tmp.ticker from (SELECT DISTINCT ON (ticker) * FROM stock_data WHERE time > now() - INTERVAL '300 days' and ticker in :tickers ORDER BY ticker, time DESC) as tmp;")
        vals = { "tickers": tuple(tickers) }
        res = conn.execute(stmt, (vals)).fetchall()
        if len(res) < len(tickers):
            df = yfDlMax(tickers=tickers, period='max', group_by="Ticker", repair=True, proxies=proxies)
        else:
            df = yfDlStart(tickers=tickers, start=findMinDate(res), group_by='Ticker', repair=True, proxies=proxies)
        tmpdf = df.stack(level=0,future_stack=True).rename_axis(['Date', 'Ticker']).reset_index(level=1).dropna().reset_index()
        diff = findBadTickers(tickers, list(tmpdf.get('Ticker').unique()))
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
        print(f"bad ticker, figure skate via xcom removal: {diff}")
        return diff

    @task.virtualenv(
            requirements=['requests'], system_site_packages=False
    )
    def rabbitmq_push_removal_q(records:list):
        print(records)

    tickers = pull_rabbitsense_batch()
    proxies = get_proxies()
    dburi = BaseHook.get_connection("stocks_ts").get_uri()
    fetch_data = getData(tickers=tickers, dburi=dburi, proxies=proxies)
    badtickers = pull_bad_tickers()
    handle_bad_tickers = rabbitmq_push_removal_q(badtickers)
    rabbitmq_sensor >> [ tickers, proxies, create_stocks_tables ] >> fetch_data
    fetch_data >> badtickers >> handle_bad_tickers

stocks_el()