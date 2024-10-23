import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import ExternalPythonOperator
from airflow.operators.bash import BashOperator

@dag(
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2024, 10, 24, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 0,
    },
    tags=["ansible"]
)

def ansible_certs_dag():

    setup_external_python = BashOperator(
        task_id="setup_external_python",
        bash_command="python -m venv /opt/airflow/ansible_venv",
        queue="ansible"
    )
    install_packages = BashOperator(
        task_id="install_packages",
        bash_command="/opt/airflow/ansible_venv/bin/pip install -r /opt/airflow/dags/pyreqs/airflow_ansible_requirements.txt",
        queue="ansible"
    )

    install_ansilbe_packages = BashOperator(
        task_id="install_ansible_packages",
        bash_command="/opt/airflow/ansible_venv/bin/pip install -r /etc/ansible/requirements.txt",
        queue="ansible"
    )

    @task.external_python(
            python="/opt/airflow/ansible_venv/bin/python3",
            expect_airflow=False,
            task_id="ansible_certstask",
            queue="ansible"
    )
    def ansible_certs():
        import json
        import yaml
        import ansible_runner
        from ansible_runner import Runner, RunnerConfig
        import os

        os.environ['PATH'] = "/opt/airflow/ansible_venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        rc = RunnerConfig(
            private_data_dir="/opt/airflow/ansible_venv/tmp",
            project_dir="/etc/ansible",
            playbook="site.yml",
            extravars='act=cert',
            limit='!skipcerts',
            only_failed_event_data=True
        )
        rc.prepare()
        r = Runner(config=rc)
        r.run()


    #cleanup_python = BashOperator(
    #    task_id="cleanup_python",
    #    bash_command="rm -Rvf /opt/airflow/ansible_venv"
    #)

    ansible_certs_task = ansible_certs()

    setup_external_python.as_setup() >> install_packages.as_setup() >> install_ansilbe_packages.as_setup() >> ansible_certs_task


ansible_certs_dag()