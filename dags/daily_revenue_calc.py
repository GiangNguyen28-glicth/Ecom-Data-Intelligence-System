from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.constants import JOB_STATUS, POSTGRESQL_CONN, REPORT_TABLE, CALC_DAILY_REVENUE_PROCESS
from helpers.helpers import Helper

default_args = {
    "owner": "giangnt",
    "retries": 2
}

def init_job_tracking(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRESQL_CONN)
    conn = hook.get_conn()
    cursor = conn.cursor()

    job_name = context["dag"].dag_id
    run_id = context["run_id"]
    current_date = Helper.get_current_date()

    cursor.execute(f"""
            INSERT INTO {REPORT_TABLE['JOBS']} (id, name, run_id, status, start_time, process_at)
            VALUES (%s, %s, %s)
            RETURNING id
        """, (job_name, run_id, JOB_STATUS["IDLE"],  current_date['timestamp'], CALC_DAILY_REVENUE_PROCESS['INIT_TRACKING_JOB']))


def update_job_tracking(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRESQL_CONN)

    tracking_id = context["ti"].xcom_pull(key="tracking_id")

    hook.run(f"""
        UPDATE {REPORT_TABLE['job']}
        SET status = {JOB_STATUS["COMPLETED"]}
        WHERE id = %s
    """, parameters=(tracking_id,))

with DAG(
        task_id="daily_revenue_calc",
        default_args=default_args,
        schedule_interval=None,
) as dag:
   init_job_tracking = PythonOperator(
        task_id="init_job_tracking",
   )