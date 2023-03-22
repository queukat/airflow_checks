from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.hive_hooks import HiveMetastoreHook
from airflow.operators.hive_operator import HiveOperator

def check_table_availability(database_list, **kwargs):
    hive_metastore_hook = HiveMetastoreHook()

    for database in database_list:
        tables = hive_metastore_hook.get_tables(db=database)
        for table in tables:
            table_status = 'AVAILABLE'
            try:
                hive_metastore_hook.get_table(database, table)
            except Exception as e:
                table_status = 'UNAVAILABLE'
            

            kwargs['ti'].xcom_push(key=f'{database}_{table}', value=table_status)

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 20),
}

dag = DAG(
    'hive_table_availability',
    default_args=default_args,
    description='Monitor Hive table availability',
    schedule_interval=timedelta(minutes=15),  # Запускать каждые 15 минут
    catchup=False,
)

database_list = ['database1', 'database2', 'database3']

check_availability = PythonOperator(
    task_id='check_table_availability',
    python_callable=check_table_availability,
    op_args=[database_list],
    provide_context=True,
    dag=dag,
)

def create_availability_report(**kwargs):
    hive_query = """
        INSERT INTO monitoring_schema.table_availability
        (database_name, table_name, status, timestamp)
        VALUES
    """

    values = []
    for database in database_list:
        tables = kwargs['ti'].xcom_pull(key=f'{database}_table', task_ids='check_table_availability')
        for table, status in tables.items():
            values.append(f"('{database}', '{table}', '{status}', '{datetime.utcnow()}')")

    hive_query += ",\n".join(values)

    return hive_query

create_report = HiveOperator(
    task_id='create_availability_report',
    hive_cli_conn_id='hive_conn_id',
    hql=create_availability_report(),
    provide_context=True,
    dag=dag,
)

check_availability >> create_report
