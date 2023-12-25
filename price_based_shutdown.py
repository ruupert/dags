#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum
import functools

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator, ExternalPythonOperator, PythonVirtualenvOperator, is_venv_installed
from airflow.operators.python import task, get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

def execute_query_with_conn_obj(query):
    hook = PostgresHook(postgres_conn_id='electricity')
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute(query)

def execute_query_with_hook(query):
    hook = PostgresHook(postgres_conn_id='electricity')
    res = hook.get_records(query)[0][1]
    return res

def next_task(task):
    context = get_current_context()

    value = shutdown_or_run.xcom_pull(context=context,key="return_value", task_ids="get_next_hour_price")
    if value > 10.0:
        return "shutdown_task"
    elif value < 2:
        return "wakeonlan_task"
    else:
        return "keep_running"
    

with DAG(
    dag_id="price_based_shutdown",
    schedule="55 * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["electricity"],
) as dag:
    hook=PostgresHook( postgres_conn_id= "electricity")
    get_next_hour_price = PythonOperator(
        task_id = "get_next_hour_price",
        op_kwargs = { 'query': "select * from electricity,(select to_timestamp(to_char((now() + interval '1 hour'), 'YYYY-MM-DD HH24:00:00'), 'YYYY-MM-DD HH24:MI:SS')) as t where t.to_timestamp = date;"},
        python_callable= execute_query_with_hook
    )
    shutdown_or_run = PythonOperator(
        task_id = "shutdown_or_run",
        op_kwargs = { 'task' : 'get_next_hour_price' },
        python_callable = next_task
    )

    def branch_func(ti):
        xcom_value = ti.xcom_pull(task_ids="shutdown_or_run")
        return xcom_value

    is_running_task = EmptyOperator(task_id='is_running_task', dag=dag)


    shutdown_task = EmptyOperator(task_id='shutdown_task', dag=dag)
    wakeonlan_task = EmptyOperator(task_id='wakeonlan_task', dag=dag)
    keep_running = EmptyOperator(task_id='keep_running', dag=dag)

    branch_task = BranchPythonOperator(
        task_id='branching',
        python_callable=branch_func,
        dag=dag,
    )
    is_running_task >> branch_task
    branch_task >> shutdown_task 
    branch_task >> wakeonlan_task
    branch_task >> keep_running

    get_next_hour_price >> shutdown_or_run >> branch_task




if __name__ == "__main__":
    dag.test()
