import datetime
import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import ExternalPythonOperator
from airflow.operators.bash import BashOperator

@dag(
    schedule="0 11 * * 5",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=30),
    },
    tags=["ansible"]
)

def ansible_update_dag():

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
            task_id="ansible_wakeonlan",
            queue="ansible",
    )
    def ansible_wol():
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
            tags='wol',
            limit='rabbit',
            only_failed_event_data=True
        )
        rc.prepare()
        r = Runner(config=rc)
        r.run()

    @task.external_python(
            python="/opt/airflow/ansible_venv/bin/python3",
            expect_airflow=False,
            task_id="ansible_apt_upgrade",
            queue="ansible"
    )
    def ansible_apt_upgrade():
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
            tags='update',
            limit='debian,freebsd',
            only_failed_event_data=True
        )
        rc.prepare()
        r = Runner(config=rc)
        r.run()


    @task.external_python(
            python="/opt/airflow/ansible_venv/bin/python3",
            expect_airflow=False,
            task_id="ansible_shutdown",
            queue="ansible"
    )
    def ansible_shutdown():
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
            tags='shutdown',
            limit='rabbit',
            only_failed_event_data=True
        )
        rc.prepare()
        r = Runner(config=rc)
        r.run()


    #cleanup_python = BashOperator(
    #    task_id="cleanup_python",
    #    bash_command="rm -Rvf /opt/airflow/ansible_venv"
    #)

    ansible_wol_task = ansible_wol()
    ansible_apt_upgrade_task = ansible_apt_upgrade()
    ansible_shutdown_task = ansible_shutdown()

    setup_external_python.as_setup() >> install_packages.as_setup() >> install_ansilbe_packages.as_setup() >> ansible_wol_task >> ansible_apt_upgrade_task >> ansible_shutdown_task


ansible_update_dag()