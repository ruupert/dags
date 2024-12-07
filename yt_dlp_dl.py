import pendulum
import json
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.dag_parsing_context import get_parsing_context
from random import randrange

current_dag_id = get_parsing_context().dag_id
channels = json.loads(Variable.get('ytchannels'))
download_dir = Variable.get('yt_download_dir')
minutes= randrange(1,59)
hour=randrange(1,23)

for channel in channels['channels']:
    dag_id = f"youtube_dl_{channel['name']}"
    if current_dag_id is not None and current_dag_id != dag_id:
        continue
    minutes += 5
    if minutes >= 60:
        minutes = minutes - 60
        hour += 1

    with DAG(
        schedule=f"{minutes} {hour} * * {channel['day']}",
        start_date=pendulum.datetime(2024, 12, 7, tz="UTC"),
        catchup=False,
        dag_id=dag_id,
        max_active_runs=1,
        default_args={
            "depends_on_past": False,
            "retries": 0,
        },
        tags=["youtube"]
    ):

        create_dir = BashOperator(
            task_id="create_download_dir",
            bash_command="mkdir {download_dir}",
            queue="youtube"
        )

        @task.virtualenv(
            requirements=['yt-dlp'],
            task_id="youtube_dl",
            queue="youtube",
            system_site_packages=False
        )
        def youtube_dl(channel, proxy, download_dir):
            import yt_dlp
            ydl_opts = {
                'format': 'bestvideo[height<=1080]+bestaudio/best[height<=1080]',
                'max_downloads': 2,
                'download_archive': f'{download_dir}/download_archive',
                'proxy': proxy,
                'break_on_existing': True,
                'outtmpl': '%(title)s.%(ext)s'
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([channel['url']])

        ytl = youtube_dl(channel, Variable.get('socksproxy'), download_dir)
        create_dir >> ytl