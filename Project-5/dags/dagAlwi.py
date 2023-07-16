import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',    
    'retry_delay': timedelta(minutes=5),
}

dag_spark = DAG(
    dag_id = "dagAlwi",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    dagrun_timeout=timedelta(minutes=60),
    description='submit project 5 in airflow',
    start_date = days_ago(1)
)

start = DummyOperator(task_id = 'start', dag=dag_spark)

spark_submit_project_5_alwi = SparkSubmitOperator(
    application="/home/dev/airflow/spark-code/project_5_alwi.py",
    conn_id="spark-standalone",
    task_id="spark_submit",
    dag=dag_spark
)

end = DummyOperator(task_id = 'end', dag=dag_spark)

start >> spark_submit_project_5_alwi >> end