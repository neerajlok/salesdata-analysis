from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta

def wait_for_two_files(files: list, **kwargs) -> bool:
    return(len(files)==2)


def move_files():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    source_bucket = 'testbucket'
    source_prefix = 'input/'
    destination_prefix = 'offload/'
    
    # List files in the source bucket
    files = s3_hook.list_keys(bucket_name=source_bucket, prefix=source_prefix)
    
    for file in files:
        # Move each file
        copy_source = {'Bucket': source_bucket, 'Key': file}
        s3_hook.copy_object(source_bucket, file, source_bucket, destination_prefix + file.split('/')[-1])
        s3_hook.delete_objects(source_bucket, keys=[file])

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG('s3_file_sensor_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Sensor to check for the presence of two CSV files
    wait_for_files = S3KeySensor(
        task_id='wait_for_files',
        bucket_key='s3://testbucket/input/*.csv',
        wildcard_match=True,
        check_fn = wait_for_two_files,
        bucket_name=None,
        timeout=120,
        poke_interval=10,
        mode='poke',
        aws_conn_id='aws_default'
    )

    # Triggers the databricks job 
    databricks_job = DatabricksRunNowOperator(
        task_id='run_dbricks_job',
        databricks_conn_id="databricks",
        job_name='sales_analysis',
        job_id=14
    )

    # Python operator to move files
    move_files_task = PythonOperator(
        task_id='move_files_task',
        python_callable=move_files
    )

    wait_for_files >> databricks_job >> move_files_task
