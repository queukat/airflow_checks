from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.models import TaskInstance, TaskFail
from airflow.utils.state import State
from airflow.utils.db import provide_session
from airflow.models import DagRun
import os
from airflow import configuration as conf

target_dag_ids = [
    'test_dag_1',
    'test_dag_2',
    'test_dag_3',
    # ...
]

max_execution_times = {
    'test_dag_1': 60,
    'test_dag_2': 120,
    'test_dag_3': 180,
    # ...
}

max_time_between_tasks = 5

monitoring_dag = DAG(
    'monitoring_dag',
    default_args=dict(
        owner='airflow',
        start_date=datetime(2022, 6, 20, 0, 0),
        email='your_email@example.com',
        email_on_failure=False,
        email_on_retry=False,
        retries=0,
    ),
    description="A monitoring DAG to supervise other DAGs",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
)

start = DummyOperator(task_id='start', dag=monitoring_dag)
end = DummyOperator(task_id='end', dag=monitoring_dag)

def check_task_start_time(dag):
    tasks = dag.tasks
    for i, task in enumerate(tasks[:-1]):
        task_instance = TaskInstance(task, dag.get_latest_execution_date())
        if task_instance.end_date is not None:
            next_task = tasks[i + 1]
            next_task_instance = TaskInstance(next_task, dag.get_latest_execution_date())
            if next_task_instance.start_date is not None:
                time_diff = (next_task_instance.start_date - task_instance.end_date).total_seconds() / 60
                if time_diff > max_time_between_tasks:
                    subject = f"АААА333 Task {next_task.task_id} is not starting in time"
                    html_content = f"Task {next_task.task_id} is not starting within {max_time_between_tasks} minutes of the completion of task {task.task_id}. Please check and resolve the issue."
                    send_email(subject, html_content)

def check_dag_execution_time(dag, max_execution_time):
    dag_run_duration = (datetime.utcnow() - dag.get_latest_execution_date()).total_seconds() / 60
    if dag_run_duration > max_execution_time:
        subject = f"АААА333 DAG {dag.dag_id} has exceeded max execution time"
        html_content = f"DAG {dag.dag_id} has exceeded the max execution time of {max_execution_time} minutes. Please check and resolve the issue."
        send_email(subject, html_content)

def get_task_logs(task_instance):
    log_base = conf.get('logging', 'base_log_folder')
    log_relative_path = task_instance.log_filepath.replace(log_base, '').lstrip('/')
    log_url = f"{os.getenv('AIRFLOW__WEBSERVER__BASE_URL')}/log?dag_id={task_instance.dag_id}&task_id={task_instance.task_id}&execution_date={task_instance.execution_date}&format=file"
    return log_url


def monitor_target_dags(**kwargs):
    for target_dag_id in target_dag_ids:
        latest_dag_run = DagRun.find(dag_id=target_dag_id, no_backfills=True, session=None)[-1]
        if latest_dag_run and latest_dag_run.get_state() != State.SUCCESS:
            check_task_start_time(dag=latest_dag_run.get_dag())

            max_execution_time = max_execution_times.get(target_dag_id)
            if max_execution_time:
                check_dag_execution_time(dag=latest_dag_run.get_dag(), max_execution_time=max_execution_time)

        failed_tasks = TaskInstance.find(dag_id=target_dag_id, state=State.FAILED)
        for task_instance in failed_tasks:
            task_fail_record = TaskFail.find(task_instance.task_instance_key)
            if task_fail_record and task_fail_record[-1].duration >= task_instance.max_retries:
                subject = f"АААА333 Task {task_instance.task_id} failed in DAG {target_dag_id}"
                html_content = f"Task {task_instance.task_id} in DAG {target_dag_id} has failed after all retries. Please check and resolve the issue. Logs: {get_task_logs(task_instance)}"
                send_email(subject, html_content)


monitor_task = PythonOperator(
    task_id='monitor_target_dags',
    python_callable=monitor_target_dags,
    provide_context=True,
    dag=monitoring_dag
)

start >> monitor_task >> end
