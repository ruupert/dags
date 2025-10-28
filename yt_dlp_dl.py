import pendulum
import json
import logging
import random
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.dag_parsing_context import get_parsing_context
from airflow.exceptions import AirflowFailException, AirflowRescheduleException
from airflow.providers.slack.operators.slack_webhook import SlackWebhookHook
from airflow.operators.empty import EmptyOperator

current_dag_id = get_parsing_context().dag_id
channels = json.loads(Variable.get('ytchannels'))
download_dir = Variable.get('yt_download_dir')
yt_url = Variable.get('yt_url')

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
            import pathlib
            import json
            import yt_dlp
            import os
            import shutil
            from datetime import datetime
            
            res  = []
            def dlhook(filename):
                res.append(filename)

            def create_tvshow_nfo(path, channel, description, thumb):
                with open(path, "w") as fh:
                    fh.writelines([
                        "<?xml version='1.0' encoding='utf-8' standalone='yes'?>",
                        "<tvshow>",
                        f"<title>{channel}</title>",
                        f"<plot>{description}</plot>",
                        f"<art><thumb>{thumb}</thumb></art>",
                        "</tvshow>\n"
                    ])
            
            def create_episode_nfo(path, title, description, channel, upload_date):
                utime = datetime.strptime(upload_date, "%Y%m%d")
                season = utime.strftime("%Y")
                aired = utime.strftime("%Y-%m-%d")
                with open(path, "w") as fh:
                    fh.writelines([
                        "<?xml version='1.0' encoding='utf-8' standalone='yes'?>",
                        "<episodedetails>",
                        f"<title>{title}</title>",
                        f"<plot>{description}</plot>",
                        f"<showtitle>{channel}</showtitle>",
                        f"<season>{season}</season>",
                        f"<aired>{aired}</aired>",
                        "</episodedetails>\n"
                    ])
            
            ydl_opts = {
              'lazy_playlist': True,
              'playlistend': 7,
              'sleep_interval': 30,
              'max_sleep_interval': 60,
              'progress_tih_newline': True,
              'ratelimit': 2200000,
              'writeinfojson': True,
              'writethumbnail': 'all',
              'ignoreerrors': 'only_download',
              'addchapters': True,
              'download_archive': f'{download_dir}/download_archive',
              'break_on_existing': True,
              'post_hooks': [dlhook],
              'outtmpl': f'{download_dir}/downloads/%(playlist)s/%(title)s-%(id)s.%(ext)s'
            }
            
            try:
              ydl = yt_dlp.YoutubeDL(ydl_opts)
              ydl.download(channel['url'])
            except yt_dlp.utils.ExistingVideoReached:
                pass
            #except yt_dlp.utils.ExtractorError:
            #    pass
            finally:
                for file in res:
                    dest_dir = os.path.dirname(file)
                    with open(file.replace(pathlib.Path(file).suffix, ".info.json"), "r") as fh:
                        file_info = json.load(fh)
                
                    path = pathlib.Path(f"{dest_dir}/tvshow.nfo")
                    if not path.is_file():
                        with open(f"{dest_dir}/{file_info['playlist']}-{file_info['playlist_id']}.info.json") as tfh:
                            tvshow_info = json.load(tfh)
                            thumb = f"{dest_dir}/{file_info['playlist']}-{file_info['playlist_id']}.jpg"
                            shutil.copy(thumb, f"{dest_dir}/default.jpg")
                            create_tvshow_nfo(path, tvshow_info['channel'], tvshow_info['description'], f"{dest_dir}/default.jpg")
                
                    episode_info = file.replace(pathlib.Path(file).suffix, ".nfo")
                    create_episode_nfo(episode_info, file_info['title'], file_info['description'], file_info['channel'], file_info['upload_date'])

        
        ytl = youtube_dl(channel, download_dir)
        create_dir >> ytl
