import pendulum
import json
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.dag_parsing_context import get_parsing_context

current_dag_id = get_parsing_context().dag_id
ansible_jobs = json.loads(Variable.get('ansible_jobs'))

for ansible_job in ansible_jobs['jobs']:
    dag_id = f"ansible_{ansible_job['name']}"
    if current_dag_id is not None and current_dag_id != dag_id:
        continue

    with DAG(
        schedule=f"{ansible_job['schedule']}",
        start_date=pendulum.datetime(2025, 3, 9, tz="UTC"),
        catchup=False,
        dag_id=dag_id,
        max_active_runs=1,
        default_args={
            "depends_on_past": False,
            "retries": 0,
        },
        tags=["ansible"]
    ):

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
                task_id="ansible_job_exectask",
                queue="ansible"
        )
        def ansible_job_exec(extravars, limit, tags, skip):
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
                extravars=extravars,
                limit=limit,
                tags=tags,
                skip_tags=skip,
                only_failed_event_data=True
            )
            rc.prepare()
            r = Runner(config=rc)
            r.run()

        ansible_job_task = ansible_job_exec(ansible_job['extravars'], ansible_job['limit'], ansible_job['tags'], ansible_job['skip'])
        setup_external_python.as_setup() >> install_packages.as_setup() >> install_ansilbe_packages.as_setup() >> ansible_job_task
