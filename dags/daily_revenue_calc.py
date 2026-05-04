import json
from dataclasses import asdict
from datetime import timedelta
from functools import partial
import boto3
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.python import PythonSensor
from psycopg2.extras import Json

from common.constants import JOB_STATUS, POSTGRESQL_CONN, CALC_DAILY_REVENUE_PROCESS, SPARK_AIRFLOW_DEFAULT_CONFIG, \
    LAST_STATE_PRODUCT_ITEM_TABLE, PRODUCT_ITEM_DAILY_TABLE, PRODUCT_ITEM_DAILY_REVENUE_TABLE, PARSED_BUCKET
from configs.settings import settings
from helpers.helpers import Helper
from model.job import Job, JobUpdate
from repositories.job_repository import JobRepository

default_args = {
    "owner": "giangnt",
    "retries": 5
}

hook = PostgresHook(postgres_conn_id=POSTGRESQL_CONN)
conn = hook.get_conn()


def init_job_tracking(**context):
    job_repo = JobRepository(conn=conn)
    job_name = context["dag"].dag_id
    run_id = context["run_id"]
    current_date = Helper.get_current_date()
    logical_date = context["logical_date"]
    process_date = logical_date.strftime("%Y-%m-%d")
    job_id = f"{job_name}_{process_date}"
    tracking_job = job_repo.find_by_id(job_id)
    if tracking_job is not None:
        print(f"Tracking job {tracking_job.id} has been initiated")
        return {
            "id": tracking_job.id,
            "status": tracking_job.status,
            "metadata": tracking_job.metadata
        }
    metadata = {
        "process_completed": ["INIT JOB COMPLETED"]
    }
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
    return {
        "id": job.id,
        "status": job.status,
        "metadata": {
            "process_completed": ["INIT JOB COMPLETED"]
        }
    }


def update_job_process(process, context):
    tracking_job = context["ti"].xcom_pull(
        task_ids="init_job_tracking"
    )
    print(f"Tracking job {tracking_job}")
    job_repo = JobRepository(conn=conn)
    job_exists = asdict(job_repo.find_by_id(tracking_job['id']))
    metadata = job_exists['metadata'] or {}
    process_completed = metadata['process_completed'] or []
    process_completed.append(process)
    metadata['process_completed'] = process_completed
    tracking_job["metadata"] = Json(metadata)
    job = JobUpdate(id=tracking_job["id"], status=JOB_STATUS["RUNNING"], metadata=tracking_job["metadata"])
    if process == CALC_DAILY_REVENUE_PROCESS["TRANSFER_DAILY_REVENUE_DATA_TO_KAFKA"]:
        job.status = JOB_STATUS["COMPLETED"]
    print(f" job {job}")
    job_repo.update(job)


def check_and_skip_if_done(process, context):
    job = context["ti"].xcom_pull(task_ids="init_job_tracking")
    metadata = job['metadata'] or {}
    process_completed = metadata['process_completed'] or []
    if process in process_completed:
        raise AirflowSkipException(f"Skipped because {process} is already completed.")


def check_data_ready(**context):
    s3 = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name="us-east-1"
    )

    process_date = context['logical_date'].strftime("%Y-%m-%d")
    # print(f"process_date: {process_date}")
    bucket = Helper.get_bucket(PARSED_BUCKET)
    year, month, day = [int(x) for x in process_date.split("-")]

    prefix = f"year={year}/month={month}/day={day}/"

    response = s3.list_objects_v2(
        Bucket=PARSED_BUCKET,
        Prefix=prefix,
        MaxKeys=1
    )
    is_existed_data = "Contents" in response
    print(f"is_existed_data: {is_existed_data}")
    return is_existed_data


with DAG(
        dag_id="daily_revenue_calc",
        default_args=default_args,
        schedule_interval=None,
        # schedule_interval="0 1 * * *",
) as dag:
    init_job = PythonOperator(
        task_id="init_job_tracking",
        python_callable=init_job_tracking,
        trigger_rule="none_failed",
    )

    wait_for_data_ready = PythonSensor(
        task_id="wait_for_data_ready",
        python_callable=check_data_ready,
        poke_interval=10,
        timeout=60 * 60 * 6,
        mode="poke",
    )

    transfer_to_iceberg_pid = SparkSubmitOperator(
        task_id='transfer_to_iceberg_pid',
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/data_transfer.py',
        deploy_mode='client',
        application_args=["transfer_to_iceberg_pid", json.dumps({
            "parsed_bucket": PARSED_BUCKET,
            "from_date": "{{ ds }}",
            "to_date": "{{ ds }}",
            "daily_table": PRODUCT_ITEM_DAILY_TABLE
        })],
        trigger_rule="none_failed",
        pre_execute=partial(check_and_skip_if_done,
                            CALC_DAILY_REVENUE_PROCESS["TRANSFER_DATA_DAILY_SNAPSHOT_TO_ICEBERG"]),
        on_success_callback=partial(
            update_job_process, CALC_DAILY_REVENUE_PROCESS["TRANSFER_DATA_DAILY_SNAPSHOT_TO_ICEBERG"]),
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )

    calc_daily_revenue = SparkSubmitOperator(
        task_id='calc_daily_revenue',
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/calc_revenue.py',
        deploy_mode='client',
        application_args=["calc_daily_sold", json.dumps({
            "process_date": "{{ ds }}",
            "state_table": LAST_STATE_PRODUCT_ITEM_TABLE,
            "daily_table": PRODUCT_ITEM_DAILY_TABLE,
            "mart_daily_revenue_table": PRODUCT_ITEM_DAILY_REVENUE_TABLE
        })],
        trigger_rule="none_failed",
        pre_execute=partial(check_and_skip_if_done,
                            CALC_DAILY_REVENUE_PROCESS["CALC_DALY_REVENUE"]),
        on_success_callback=partial(
            update_job_process, CALC_DAILY_REVENUE_PROCESS["CALC_DALY_REVENUE"]),
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )

    transfer_to_iceberg_latest_pi = SparkSubmitOperator(
        task_id='transfer_to_iceberg_latest_pi',
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/data_transfer.py',
        deploy_mode='client',
        application_args=["transfer_to_iceberg_latest_pi", json.dumps({
            "process_date": "{{ ds }}",
            "daily_table": PRODUCT_ITEM_DAILY_TABLE,
            "state_table": LAST_STATE_PRODUCT_ITEM_TABLE
        })],
        trigger_rule="none_failed",
        pre_execute=partial(check_and_skip_if_done,
                            CALC_DAILY_REVENUE_PROCESS["TRANSFER_DATA_DAILY_SNAPSHOT_TO_LATEST"]),
        on_success_callback=partial(
            update_job_process, CALC_DAILY_REVENUE_PROCESS["TRANSFER_DATA_DAILY_SNAPSHOT_TO_LATEST"]),
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )

    pid_revenue_2_kafka = SparkSubmitOperator(
        task_id='pid_revenue_2_kafka',
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/iceberg_2_kafka.py',
        deploy_mode='client',
        trigger_rule="none_failed",
        application_args=["pid_revenue_2_kafka", json.dumps({
            "process_date": "{{ ds }}",
            "batch": 10,
            "mart_daily_revenue_table": PRODUCT_ITEM_DAILY_REVENUE_TABLE
        })],
        pre_execute=partial(check_and_skip_if_done,
                            CALC_DAILY_REVENUE_PROCESS["TRANSFER_DAILY_REVENUE_DATA_TO_KAFKA"]),
        on_success_callback=partial(
            update_job_process, CALC_DAILY_REVENUE_PROCESS["TRANSFER_DAILY_REVENUE_DATA_TO_KAFKA"]),
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )
    wait_for_data_ready >> init_job >> transfer_to_iceberg_pid >> calc_daily_revenue >> transfer_to_iceberg_latest_pi >> pid_revenue_2_kafka
