#pip install 'apache-airflow[amazon]'
from airflow.providers.amazon.aws.operators.batch import  BatchOperator
from airflow import DAG
from datetime import datetime, timedelta

dag_licop = DAG(
    dag_id="licop_job_execution",
    description="DAG calling AWS Batch Job for Spark to Snowflake execution",
    default_args={"owner": "Airflow"},
    schedule_interval="@once",
    catchup=False,
    start_date=datetime(1979,4,28),
)


licop_submit_batch_job = BatchOperator(
    task_id="licop_submit_batch_job",
    job_name="Licop_job_name",
    job_queue="academy-capstone-winter-2024-job-queue",
    job_definition="Licop_job_name",
    dag=dag_licop

)

licop_submit_batch_job