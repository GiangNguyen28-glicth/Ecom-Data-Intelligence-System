import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

from common.constants import SPARK_AIRFLOW_DEFAULT_CONFIG, PARSED_BUCKET_STAGING, LAST_STATE_PRODUCT_ITEM_STAGING_TABLE, \
    PRODUCT_ITEM_DAILY_STAGING_TABLE, PRODUCT_ITEM_DAILY_REVENUE_STAGING_TABLE

# ===== CONFIG =====
FROM_DATE = "2026-04-21"
TO_DATE = "2026-04-24"


def generate_dates():
    start = datetime.fromisoformat(FROM_DATE)
    end = datetime.fromisoformat(TO_DATE)

    dates = []
    cur = start
    while cur <= end:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    return dates


dates = generate_dates()

with DAG("backfill_transform_raw_data") as dag:
    transform_raw_2_parsed_bucket = SparkSubmitOperator(
        task_id="transform_raw_2_parsed_data",
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/transform_raw_data.py',
        deploy_mode='client',
        application_args=["transform_raw_2_parsed_data", json.dumps({
            "from_date": FROM_DATE,
            "to_date": TO_DATE,
            "target_bucket": PARSED_BUCKET_STAGING
        })],
        trigger_rule="none_failed",
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )
    create_state_staging_table = SparkSubmitOperator(
        task_id="create_state_product_item_staging",
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/backfill_rebuild_state_product_item.py',
        deploy_mode='client',
        application_args=["create_state_product_item_staging", json.dumps({
            "from_date": FROM_DATE,
            "state_product_item_staging_table": LAST_STATE_PRODUCT_ITEM_STAGING_TABLE
        })],
        trigger_rule="none_failed",
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )

    transfer_to_iceberg_pid = SparkSubmitOperator(
        task_id='transfer_to_iceberg_pid',
        conn_id='spark_default',
        application='/opt/airflow/project/jobs/data_transfer.py',
        deploy_mode='client',
        application_args=["transfer_to_iceberg_pid", json.dumps({
            "parsed_bucket": PARSED_BUCKET_STAGING,
            "from_date": FROM_DATE,
            "to_date": TO_DATE,
            "daily_table": PRODUCT_ITEM_DAILY_STAGING_TABLE
        })],
        trigger_rule="none_failed",
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
            "from_date": FROM_DATE,
            "to_date": TO_DATE,
            "output_topic": "clickhouse_ingest_topic_staging",
            "batch": 10,
            "mart_daily_revenue_table": PRODUCT_ITEM_DAILY_REVENUE_STAGING_TABLE
        })],
        conf={
            **SPARK_AIRFLOW_DEFAULT_CONFIG
        }
    )

    task_groups = []

    for d in dates:
        with TaskGroup(group_id=f"process_{d.replace('-', '_')}") as tg:
            calc_revenue = SparkSubmitOperator(
                task_id="calc_daily_revenue",
                conn_id="spark_default",
                application="/opt/airflow/project/jobs/calc_revenue.py",
                deploy_mode="client",
                application_args=[
                    "calc_daily_sold",
                    json.dumps({
                        "process_date": d,
                        "state_table": LAST_STATE_PRODUCT_ITEM_STAGING_TABLE,
                        "daily_table": PRODUCT_ITEM_DAILY_STAGING_TABLE,
                        "mart_daily_revenue_table": PRODUCT_ITEM_DAILY_REVENUE_STAGING_TABLE
                    })
                ],
                conf={**SPARK_AIRFLOW_DEFAULT_CONFIG},
                pool="spark_sequential"
            )

            update_state = SparkSubmitOperator(
                task_id="update_latest_state",
                conn_id="spark_default",
                application="/opt/airflow/project/jobs/data_transfer.py",
                deploy_mode="client",
                application_args=[
                    "transfer_to_iceberg_latest_pi",
                    json.dumps({
                        "process_date": d,
                        "daily_table": PRODUCT_ITEM_DAILY_STAGING_TABLE,
                        "state_table": LAST_STATE_PRODUCT_ITEM_STAGING_TABLE
                    })
                ],
                conf={**SPARK_AIRFLOW_DEFAULT_CONFIG},
                pool="spark_sequential"
            )

            calc_revenue >> update_state

        task_groups.append(tg)

    # ===== DAG dependency =====
    chain(
        transform_raw_2_parsed_bucket,
        create_state_staging_table,
        transfer_to_iceberg_pid,
        *task_groups,
        pid_revenue_2_kafka
    )
