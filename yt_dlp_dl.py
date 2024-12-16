import pendulum
import json
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.dag_parsing_context import get_parsing_context

current_dag_id = get_parsing_context().dag_id
channels = json.loads(Variable.get('ytchannels'))
download_dir = Variable.get('yt_download_dir')

for channel in channels['channels']:
    dag_id = f"youtube_dl_{channel['name']}"
    if current_dag_id is not None and current_dag_id != dag_id:
        continue

    with DAG(
        schedule=f"{channel['min']} {channel['hour']} * * {channel['day']}",
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
            bash_command=f"mkdir -p {download_dir}/downloads; echo 'success'",
            queue="youtube"
        )

        @task.virtualenv(
            requirements=['yt-dlp'],
            task_id="youtube_dl",
            queue="youtube",
            system_site_packages=False
        )
        def youtube_dl(channel, download_dir):
            import yt_dlp
            ydl_opts = {
                'lazy_playlist': True,
                'cachedir': f'{download_dir}/cache',
                'playlistend': 5,
                'sleep_interval': 30,
                'max_sleep_interval': 60,
                'ratelimit': 500000,
                'download_archive': f'{download_dir}/download_archive',
                'break_on_existing': True,
                'outtmpl': f'{download_dir}/downloads/%(playlist)s/%(title)s.%(ext)s',
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    ydl.download([channel['url']])
                except yt_dlp.utils.ExistingVideoReached:
                    return 0

        ytl = youtube_dl(channel, download_dir)
        create_dir >> ytl
