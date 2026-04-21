from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json
from common.constants import JOB_STATUS, POSTGRESQL_CONN, CALC_DAILY_REVENUE_PROCESS, SPARK_AIRFLOW_DEFAULT_CONFIG
from helpers.helpers import Helper
from model.job import Job
from repositories.job_repository import JobRepository

default_args = {
    "owner": "giangnt",
    "retries": 5
}

def init_job_tracking(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRESQL_CONN)
    conn = hook.get_conn()
    job_repo = JobRepository(conn=conn)

    job_name = context["dag"].dag_id
    run_id = context["run_id"]
    current_date = Helper.get_current_date()
    logical_date = context["logical_date"]
    process_date = logical_date.strftime("%Y-%m-%d")
    job_id = f"{job_name}_{process_date}"
    tracking_job = job_repo.find_by_id(job_id)
    metadata = {
        "process_completed": ["INIT JOB COMPLETED"],
    }
    if tracking_job is not None:
        print(f"Tracking job {tracking_job.id} has been initiated")
        return
    job = Job(
        name=job_name,
        run_id=run_id,
        status=JOB_STATUS["IDLE"],
        start_time=current_date['timestamp'],
        process_at=CALC_DAILY_REVENUE_PROCESS["INIT_TRACKING_JOB"],
        id=job_id,
        metadata=Json(metadata)
    )

    job_repo.create(job)

with DAG(
        dag_id="daily_revenue_calc",
        default_args=default_args,
        schedule_interval=None,
) as dag:
    init_job = PythonOperator(
        task_id="init_job_tracking",
        python_callable=init_job_tracking,
    )

    transfer_to_iceberg_pid = SparkSubmitOperator(
        task_id='transfer_to_iceberg_pid',
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/data_transfer.py',
        deploy_mode='client',
        application_args=["transfer_to_iceberg_pid"],
        conf={
           **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )

    calc_daily_revenue = SparkSubmitOperator(
        task_id='calc_daily_revenue',
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/calc_revenue.py',
        deploy_mode='client',
        application_args=["calc_daily_sold"],
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )

    transfer_to_iceberg_latest_pi = SparkSubmitOperator(
        task_id='transfer_to_iceberg_latest_pi',
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/data_transfer.py',
        deploy_mode='client',
        application_args=["transfer_to_iceberg_latest_pi"],
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )

    pid_revenue_2_kafka = SparkSubmitOperator(
        task_id='pid_revenue_2_kafka',
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/iceberg_2_kafka.py',
        deploy_mode='client',
        application_args=["pid_revenue_2_kafka"],
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )

    init_job >> transfer_to_iceberg_pid >> calc_daily_revenue >> transfer_to_iceberg_latest_pi >> pid_revenue_2_kafka
