from datetime import datetime, timedelta
from pytz import timezone
import requests
import logging
import traceback
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1, tzinfo=timezone('Europe/London')),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email' : '123@123.123'
}

dag = DAG(
    'airflow_tasks',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
)

start = DummyOperator(task_id='start', dag=dag)

finish = DummyOperator(task_id='finish', dag=dag)

create_table_query = """
    CREATE TABLE IF NOT EXISTS database.table (
        dag_id STRING,
        task_id STRING,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        duration FLOAT,
        status STRING
    )
    PARTITIONED BY (date STRING)
"""
create_table = HiveOperator(
    task_id='create_hive_table',
    hql=create_table_query,
    dag=dag,
)

def get_tasks(ds, **kwargs):
    airflow_api_url = 'https://airflow/api/'
    response = requests.get(f'{airflow_api_url}/dags/my_dag/dagRuns?limit=1', verify=False)
    dag_run_id = response.json()['dag_runs'][0]['dag_run_id']
    response = requests.get(f'{airflow_api_url}/dags/my_dag/dagRuns/{dag_run_id}/taskInstances', verify=False)
    return response.json()

def write_tasks_to_hive(ds, **kwargs):
    tasks = get_tasks(ds, **kwargs)
    hive_tasks = []
    for task in tasks:
        hive_tasks.append(
            f"""
            INSERT INTO database.table PARTITION (date='{ds}')
            VALUES (
                '{task["dag_id"]}',
                '{task["task_id"]}',
                '{task["start_date"]}',
                '{task["end_date"] if task["end_date"] else datetime.utcnow()}',
                TIMESTAMPDIFF(MINUTE, '{task["start_date"]}', '{task["end_date"] if task["end_date"] else datetime.utcnow()}'),
                '{task["state"]}'
            )
            """
        )
    kwargs['ti'].xcom_push(key='hive_tasks', value=hive_tasks)

write_tasks_op = PythonOperator(
    task_id='write_tasks_to_hive',
    python_callable=write_tasks_to_hive,
    provide_context=True,
    dag=dag,
)

write_to_hive = HiveOperator(
    task_id='write_to_hive',
    op_args=['{{ ti.xcom_pull(task_ids="write_tasks_to_hive", key="hive_tasks") }}'],
    hql=';\n'.join('{{ task_instance.xcom_pull(task_ids="write_tasks_to_hive", key="hive_tasks")[{}] }}'.format(i) for i in range(len('{{ ti.xcom_pull(task_ids="write_tasks_to_hive", key="hive_tasks") }}'))),
    dag=dag,
)

tz = timezone("Europe/London")

def check_dag_status(**kwargs):
    dag = kwargs['dag']
    tasks = dag.tasks
    unfinished_tasks = [t for t in tasks if t.task_id != 'check_dag_status' and t.end_date is None]
if unfinished_tasks:
current_time = datetime.now(tz).time()
if current_time > time(10, 0):
subject = f"DAG {dag.dag_id} has unfinished tasks"
html_content = f"DAG {dag.dag_id} has unfinished tasks and it's past 10:00 AM in Moscow time. Please check and resolve the issue."
return {'subject': subject, 'html_content': html_content}
send_email(subject, html_content)

def check_task_start_time(**kwargs):
dag = kwargs['dag']
tasks = dag.tasks
for i, task in enumerate(tasks[:-1]):
if task.end_date is not None:
next_task = tasks[i + 1]
if next_task.start_date is not None:
time_diff = (next_task.start_date - task.end_date).total_seconds() / 60
if time_diff > 5:
subject = f"Task {next_task.task_id} is not starting in time"
html_content = f"Task {next_task.task_id} is not starting within 5 minutes of the completion of task {task.task_id}. Please check and resolve the issue."
return {'subject': subject, 'html_content': html_content}
send_email(subject, html_content)

class SparkShellOperator(BaseOperator):
template_fields = ('spark_command',)
template_ext = ('.sh',)
@apply_defaults
def __init__(self, spark_command, email, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.spark_command = spark_command
    self.email = email

def execute(self, context):
    script_file = f"/tmp/{self.task_id}.scala"
    with open(script_file, 'w') as f:
        f.write(self.spark_command)
    bash_command = f"spark-shell --driver-memory 5g --executor-memory 2g --num-executors 1 -i {script_file}"
    try:
        BashOperator(task_id=self.task_id, bash_command=bash_command, dag=self.dag).execute(context)
    except Exception as e:
        logging.error("Failed to run Spark command: %s", str(e))
        logging.error("Stack trace: %s", traceback.format_exc())
        send_email(to=self.email, subject="Airflow task failed",
                   html_content="<pre>" + traceback.format_exc() + "</pre>")
        raise e
email = '123@123.123'

spark_task = SparkShellOperator(
task_id='spark_task',
spark_command="""
spark.sql("SELECT COUNT(*) FROM database.table")
System.exit(0)""",
email=email,
dag=dag
)

start >> create_table >> write_tasks_op >> write_to_hive >> finish

check_dag_status_op = PythonOperator(
task_id='check_dag_status',
provide_context=True,
python_callable=check_dag_status,
dag=dag
)

check_task_start_time_op = PythonOperator(
task_id='check_task_start_time',
provide_context=True,
python_callable=check_task_start_time,
dag=dag
)

check_dag_status_op >> finish
check_task_start_time_op >> finish
