import pendulum
from datetime import timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from rabbitmq_provider.sensors.rabbitmq import RabbitMQSensor
from rabbitmq_provider.operators.rabbitmq import RabbitMQHook

@dag(
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 2, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=1),
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["electricity"],
)

def shelly_el():
    rmq_hook = RabbitMQHook(
        rabbitmq_conn_id="rmq_shelly"
    )
    sql_hook = PostgresHook(
        postgres_conn_id="shelly"
    )
    sense_rmq_shelly = RabbitMQSensor(
        task_id="sense_rmq_shelly",
        queue_name="metrics",
        rabbitmq_conn_id="rmq_shelly"
    )

    #create_shelly_tables = PostgresOperator(
    #    task_id="create_shelly_tables",
    #    postgres_conn_id="shelly",
    #    sql="sql/shelly_metrics_schema.sql",
    #)

    @task.python(
        task_id="nop"
    )
    def nopf():
        import json
        import datetime
        try:
            msg = json.loads(rmq_hook.pull(queue_name="metrics"))
        except:
            return
        count = 0
        tf=( "time", "name", "output", "apower", "voltage", "freq", "current", "temperature")
        rs=[]
        while msg != None:
            rs.append((datetime.datetime.fromtimestamp((float(msg['timestamp'])/1000)),
                msg['name'],
                msg['output'],
                msg['apower'],
                msg['voltage'],
                msg['freq'],
                msg['current'],
                msg['temperature']['tC']
            ))
            count+=1
            if count >= 1000:
                break
            try:
                msg = json.loads(rmq_hook.pull(queue_name="metrics"))
            except:
                break
        
        sql_hook.insert_rows(
            table="shelly_powers",
            executemany=True,
            autocommit=True,
            rows=rs,
            target_fields=tf
        )

    nop = nopf()
    sense_rmq_shelly >> nop

shelly_el()
