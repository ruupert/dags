import datetime
import pendulum

from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.hook import BaseHook

@dag(
    schedule="25 13 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 20,
        "retry_delay": datetime.timedelta(minutes=10),
    },
    tags=["finance"],
)
def stocks_el():
    create_stocks_tables = SQLExecuteQueryOperator(
        task_id="create_stocks_tables",
        conn_id="stocks_ts",
        sql="""
            CREATE TABLE IF NOT EXISTS stock_data ( 
                    time timestamp not null, 
                    open FLOAT, 
                    high FLOAT, 
                    low FLOAT, 
                    close FLOAT, 
                    adjclose FLOAT,
                    repaired boolean,
                    volume FLOAT, 
                    ticker TEXT 
            );
            CREATE EXTENSION IF NOT EXISTS timescaledb;
            SELECT create_hypertable('stock_data', by_range('time'), if_not_exists => TRUE);
            CREATE MATERIALIZED VIEW IF NOT EXISTS minmax AS
                    SELECT ticker, min(close), max(close) FROM stock_data GROUP BY ticker;
            CREATE OR REPLACE FUNCTION public.first_agg (anyelement, anyelement)
            RETURNS anyelement
            LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
            'SELECT $1';
            CREATE OR REPLACE AGGREGATE public.first (anyelement) (
            SFUNC    = public.first_agg
            , STYPE    = anyelement
            , PARALLEL = safe
            );
            CREATE OR REPLACE FUNCTION public.last_agg (anyelement, anyelement)
            RETURNS anyelement
            LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS
            'SELECT $2';
            CREATE OR REPLACE AGGREGATE public.last (anyelement) (
            SFUNC    = public.last_agg
            , STYPE    = anyelement
            , PARALLEL = safe
            );
            CREATE MATERIALIZED VIEW IF NOT EXISTS last_close AS
                    select ticker, last(close) from stock_data group by ticker;
        """,
    )

    @task.virtualenv(
        task_id="fetch_data",
        requirements=["psycopg2_binary==2.9.9", "SQLAlchemy==2.0.31", "yfinance", "scipy"], 
        system_site_packages=False,
        do_xcom_push=True,
    )
    def getData(tickers, dburi):
        import yfinance as yf
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

        def yfDlMax(tickers, period, group_by, repair):
            print("Selected download method: PERIOD")
            return yf.download(tickers=tickers, period=period, group_by=group_by, repair=repair)
        def yfDlStart(tickers, start, group_by, repair):
            print("Selected download method: START")
            return yf.download(tickers=tickers, start=start, group_by=group_by, repair=repair)

        engine = sqlalchemy.create_engine(url=dburi.replace("postgres://", "postgresql://", 1))
        with engine.raw_connection() as conn:
            stmt = text("select tmp.time, tmp.ticker from (SELECT DISTINCT ON (ticker) * FROM stock_data WHERE time > now() - INTERVAL '300 days' and ticker in :tickers ORDER BY ticker, time DESC) as tmp;")
            vals = { "tickers": tuple(tickers) }
            res = conn.execute(stmt, (vals)).fetchall()
            conn.close()
            print(f"TICKERS: {tickers}")
            print(f"TICKER COUNT IN DB: {len(res)}, TICKERS IN {len(tickers)}")
            if len(res) < len(tickers):
                df = yfDlMax(tickers=tickers, period='max', group_by="Ticker", repair=False)
            else:
                df = yfDlStart(tickers=tickers, start=findMinDate(res), group_by='Ticker', repair=False)
            if df.empty:
                exit(1)
            print(f"GOT:\n{df}\n")
        # todo: handle case with single ticker downloaded, different transform than with multiples.
        tmpdf = df.stack(level=0,future_stack=True).rename_axis(['Date', 'Ticker']).reset_index(level=1).dropna().reset_index()
        print(f"TMPDF INITIAL TRANSFORM:\n{tmpdf}\n\nTMPDF COLUMNS:\n{tmpdf.columns}")
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
        print(f"TMPDF AFTER RENAMES:\n{tmpdf}\n\nTMPDF COLUMNS:\n{tmpdf.columns}")
        tmpdf.to_sql(name='stock_data', 
            con=engine,schema='public',
            if_exists='append',
            index=False,
            method=insert_on_conflict_nothing)
        conn.close()

    refresh_materialized_view = SQLExecuteQueryOperator(
        task_id="refresh_materialized_view",
        conn_id="stocks_ts",
        sql="refresh materialized view minmax;",
    )

    refresh_materialized_view_close = SQLExecuteQueryOperator(
        task_id="refresh_materialized_view_close",
        conn_id="stocks_ts",
        sql="refresh materialized view last_close;",
    )

    dburi = BaseHook.get_connection("stocks_ts").get_uri()
    tickers = Variable.get(key="stocks", deserialize_json=True)
    fetch_data = getData(tickers=tickers['tickers'], dburi=dburi)
    create_stocks_tables >> fetch_data >> refresh_materialized_view >> refresh_materialized_view_close


stocks_el()
