import datetime
import pendulum
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed

@dag(
    schedule="25 20 * * *",
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
        sql="sql/google_sheets_stocks_el_schema.sql",
    )

    @task.virtualenv(
        requirements=['-r /opt/airflow/dags/pyreqs/google_sheets_stocks_el.txt'], system_site_packages=False
    )
    def extract(account, hook):

        import os.path
        import yaml
        import json

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from google.auth.transport.requests import Request
        from google.oauth2.service_account import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
        from googleapiclient import discovery
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        SAMPLE_RANGE_NAME = 'A2:B99999'

        creds = Credentials.from_service_account_info(json.loads(account))
        tickers = hook.get_records("SELECT * FROM tickers;")
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            for item in tickers:
                result = sheet.values().get(spreadsheetId=item['sheet'],
                                            range=SAMPLE_RANGE_NAME).execute()
                values = result.get('values', [])
                if not values:
                    print('No data found.')
                    return
                for row in values:
                    data_tuple = (row[0],item['ticker'], row[1])
                    execute_query_with_conn_obj("""INSERT INTO prices (date, ticker, price) VALUES (%s, %s, %s)""",data_tuple, hook)
        except HttpError as err:
            print(err)
    etl = extract(account=Variable.get("google_sheets_account"),
                                hook=hook)
    create_stocks_tables >> etl

google_sheets_stocks_el()
