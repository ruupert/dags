import datetime
import pendulum
from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    schedule="5 */8 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["electricity"],
)

def helen_el():
    hook = PostgresHook(postgres_conn_id='electricity')

    def execute_query_with_conn_obj(query, datatuple, hook):
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(query, datatuple)
        conn.commit()

    create_electricity_tables = SQLExecuteQueryOperator(
        task_id="create_electricity_tables",
        conn_id="electricity",
        sql="""
            CREATE TABLE IF NOT EXISTS consumption (date timestamp NOT NULL, val REAL NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_consumption_date ON consumption (date);
        """,
    )
    @task.virtualenv(
        requirements=["pandas",
                "Numpy",
                "PyYAML",
                "requests",
                "psycopg2-binary",
                "selenium",
                "bz2file",
                "openpyxl"],
        queue="selenium",
        system_site_packages=False
    )
    def extract(username, password):
        import datetime
        import math
        import os
        import time
        from datetime import datetime, timedelta, timezone
        from pathlib import Path
        import pandas
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.chromium.options import ChromiumOptions as Options
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.support.ui import WebDriverWait

        os.environ['PATH'] = "$PATH:/usr/bin:/usr/sbin:/usr/local/bin:/bin:/sbin"

        t = datetime.now()
        start_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0, tzinfo=timezone.utc) + timedelta(days=-10)
        end_date = datetime(year=t.year,month=t.month,day=t.day, hour=0, minute=0, second=0, tzinfo=timezone.utc)

        FPATH = f"/opt/airflow/Downloads/electricity_report_{ start_date.strftime('%d-%m-%Y') }_{ end_date.strftime('%d-%m-%Y') }.xlsx"
        def init_geckodriver(verbose=0):
            options = Options()
            options.add_argument("--temp-profile")
            if verbose == 0:
                options.add_argument("-headless")
            options.add_experimental_option(  
                "prefs", {  
                    "download.prompt_for_download": False,
                    "download.directory_upgrade": True,  
                    "safebrowsing.enabled": True
                }  
            )  
            return webdriver.Chrome(options=options)


        def get_consumption(driver, start_date):
            end = datetime.now().strftime("%Y-%m-%d")
            driver.get(
                f"https://web.oma.helen.fi/personal/reports/electricity-consumption?resolution=day&date={end}"
            )
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "recharts-wrapper"))
            )
            els = driver.find_elements(By.TAG_NAME, 'button')
            for elem in els:
                if elem.get_attribute('data-testid') == 'download-excel':
                    elem.click()
                    print("cliecked: download-excel")
                    break

            els = driver.find_elements(By.TAG_NAME, 'button')
            ins = driver.find_element(By.ID, 'excel-download-date-range-start')
            ins.send_keys(Keys.CONTROL, "a", Keys.DELETE)
            ins.send_keys(start_date.strftime("%d.%m.%Y"))
            print("inserted start date")

            for elem in els:
                print(elem.get_attribute('data-testid'))
                if elem.get_attribute('data-testid') == 'excelDataResolutionSelect-toggle-button':
                    elem.click()
                    print("cliecked: excelDataResolutionSelect-toggle-button")
                    break

            # hourly: excelDataResolutionSelect-item-hour  15min: excelDataResolutionSelect-item-quarter
            divs = driver.find_elements(By.TAG_NAME, 'div')
            for div in reversed(divs):
                div.get_attribute('data-testid')
                if div.get_attribute('data-testid') == 'excelDataResolutionSelect-item-hour':
                    div.click()
                    print("cliecked: excelDataResolutionSelect-item-hour")
                    break

            els = driver.find_elements(By.TAG_NAME, 'button')
            for elem in reversed(els):
                if elem.get_attribute('data-testid') == 'excel-download-modal-primary-button':
                    elem.click()
                    print("cliecked: excel-download-modal-primary-button")
                    break

            file_exists = False
            for i in range(1, 15):
                time.sleep(5)
                if os.path.exists(FPATH):
                    file_exists = True
                    print(FPATH)
                    break

            if not file_exists:
                raise ValueError('Download probably failed.')

        def login(driver:webdriver.Chrome, username, password):
            driver.get("https://www.helen.fi/kirjautuminen")
            off_frame_els = driver.find_elements(By.TAG_NAME, 'button')
            for elem in reversed(off_frame_els):
                if elem.accessible_name == 'Vain välttämättömät':
                    elem.click()
                    print("cookies dismissed")
            driver.switch_to.frame(0)
            driver.find_element(By.NAME, "username").send_keys(username)
            driver.find_element(By.NAME, "password").send_keys(password)
            driver.find_element(By.NAME, "password").send_keys(Keys.ENTER)
            print("entered credentials submitted login")

            time.sleep(20)
            driver.switch_to.default_content()
            ## fix later:
            #try: 
            #    WebDriverWait(driver, 3).until(
            #        EC.presence_of_element_located((By.CLASS_NAME, "recharts-wrapper"))
            #    )
            #except Exception as ex:
            #    raise ex

        driver = init_geckodriver()
        login(driver, username, password)

        try:
            get_consumption(driver=driver, start_date=start_date)
            pd_excel_df = pandas.read_excel(Path(FPATH))
            print(pd_excel_df)
            data_tuples = []
            for row in pd_excel_df.itertuples():
                if math.isnan(row[2]):
                    break
                else:
                    data_tuple = (row[1].strftime("%Y-%m-%d %H:%M:%S"), row[2])
                    data_tuples.append(data_tuple)

            os.remove(Path(FPATH))
            driver.close()
            return data_tuples
        except Exception as ex:
            driver.close()
            raise ex

    @task()
    def load_consumption(data_tuples, hook: PostgresHook):
        for data_tuple in data_tuples:
            execute_query_with_conn_obj("""INSERT INTO consumption (date, val) VALUES (%s, %s) ON CONFLICT (date) DO NOTHING""", data_tuple, hook)

    consumption_data = extract( Variable.get("electricity_costs_helen_username"),
                                Variable.get("electricity_costs_helen_password"))
    load_data = load_consumption(consumption_data, hook)
    create_electricity_tables >> consumption_data >> load_data

helen_el()
