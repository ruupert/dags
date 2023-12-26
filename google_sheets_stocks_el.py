import datetime
import pendulum
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed

@dag(
    schedule="25 13 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["stocks"],
)

def google_sheets_stocks_el():
    hook = PostgresHook(postgres_conn_id='stocks')

    def execute_query_with_conn_obj(query, datatuple, hook):
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(query, datatuple)
        conn.commit()


    create_stocks_tables = PostgresOperator(
        task_id="create_stocks_tables",
        postgres_conn_id="stocks",
        sql="sql/google_scheets_stocks_el_schema.sql",
    )

    get_tickers = PostgresOperator(
        task_id="get_tickers",
        postgres_conn_id="postgres_default",
        sql="SELECT * FROM tickers;",
    )


    @task.virtualenv(
        requirements=['-r /opt/airflow/dags/pyreqs/google_sheets_stocks_el.txt'], system_site_packages=False
    )
    def extract(account, tickers):
        from __future__ import print_function

        import os.path
        import yaml

        from google.auth.transport.requests import Request
        from google.oauth2.service_account import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
        from googleapiclient import discovery

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        SAMPLE_RANGE_NAME = 'A2:B99999'

        creds = Credentials.from_service_account_info(account)
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            for item in conf['tickers']:
                result = sheet.values().get(spreadsheetId=item['sheet'],
                                            range=SAMPLE_RANGE_NAME).execute()
                values = result.get('values', [])

                if not values:
                    print('No data found.')
                    return

                for row in values:
                    # Print columns A and E, which correspond to indices 0 and 4.
                    db.insert_or_update(row, item['ticker'])
                    #print('%s, %s' % (row[0], row[4]))
        except HttpError as err:
            print(err)


    @task()
    def load_consumption(df, hook: PostgresHook):
        for row in df.itertuples():
            if row[1]['status'] == 'valid':
                data_tuple = (row[0].tz_localize('utc').tz_convert("Europe/Helsinki"), row[1]['value'])
                execute_query_with_conn_obj("""INSERT INTO consumption (date, val) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING""", data_tuple, hook)

    tickers = get_tickers()
    consumption_data = extract(account=Variable.get("google_sheets_account"),
                                tickers=tickers)
    create_stocks_tables >> get_tickers >> 
    load_consumption(consumption_data, hook)
    load_prices(price_data, hook)

google_sheets_stocks_el()
