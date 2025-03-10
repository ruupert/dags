import pendulum
import json
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
            system_site_packages=False,
            provide_context=True
        )
        def youtube_dl(channel, download_dir):
            import yt_dlp
            import datetime
            ydl_opts = {
                'lazy_playlist': True,
                'cachedir': f'{download_dir}/cache',
                'playlistend': 5,
                'sleep_interval': 30,
                'max_sleep_interval': 60,
                'progress_with_newline': True,
                'ratelimit': 1200000,
                'download_archive': f'{download_dir}/download_archive',
                'break_on_existing': True,
                'outtmpl': f'{download_dir}/downloads/%(playlist)s/{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")}_%(title)s.%(ext)s',
            }
            dlcount = 0
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                try:
                    ydl.download([channel['url']])
                    ydl._format_out()
                    dlcount += 1
                except yt_dlp.utils.ExistingVideoReached:
                    return { 'dlcount': dlcount, 'err': 0 }
                except yt_dlp.utils.DownloadError as e:
                    if e.msg.__contains__('members'):
                        return { 'dlcount': dlcount, 'err': 0 }
                    else:
                        return { 'dlcount': dlcount, 'err': 3 }
                except yt_dlp.utils.ExtractorError:
                    return { 'dlcount': dlcount, 'err': 2 }
                except Exception:
                    return { 'dlcount': dlcount, 'err': 2 }
            return { 'dlcount': dlcount, 'err': 0 }
        
        @task.python(
            task_id='webhook_and_break',
            queue="youtube",
        )
        def webhook_and_break(ytl, hook):
            if ytl['dlcount'] > 0:
                hook.send_text(f"{channel['name']}: {ytl['dlcount']} video(s) downloaded")  
            if ytl['err'] == 3:            
                raise AirflowRescheduleException
            if ytl['err'] == 2:
                raise AirflowFailException
            return 0
        hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")  
        ytl = youtube_dl(channel, download_dir)
        webhook_and_break_task = webhook_and_break(ytl, hook)
        create_dir >> ytl >> webhook_and_break_task
