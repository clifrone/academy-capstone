#pip install 'apache-airflow[amazon]'

from airflow import DAG
from datetime import datetime, timedelta

from conveyor.operators import ConveyorContainerOperatorV2

dag_licop = DAG(
    dag_id="licop_job_execution",
    description="DAG calling AWS Batch Job for Spark to Snowflake execution",
    default_args={"owner": "Airflow"},
    schedule_interval="@once",
    catchup=False,
    start_date=datetime(1979,4,28),
)


licop_submit_batch_job = ConveyorContainerOperatorV2(
    task_id="licop_submit_batch_job",
    aws_role="capstone_conveyor",
    instance_type='mx.micro',
    dag=dag_licop

)

licop_submit_batch_job