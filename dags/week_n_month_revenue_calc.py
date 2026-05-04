from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.python import PythonSensor
import clickhouse_connect
from docker.types import Mount
import requests


from configs.settings import settings

default_args = {
    "owner": "giangnt",
    "retries": 5
}

def is_synced_success_kafka_2_clickhouse(partition):
    if partition.topic != "clickhouse_ingest_topic_staging":
        return False
    return partition["currentOffset"] == partition["endOffset"]

def validate_data_sync_to_clickhouse(**kwargs):
    try:
        consumer_group_url = "/api/clusters/My%20Kafka%20Cluster/consumer-groups/clickhouse_consumer_group"
        res = requests.get(f"{settings.kafka_api_url}{consumer_group_url}", timeout=30)
        res.raise_for_status()
        partitions = res.json()["partitions"]
        print("Get kafka partitions")
        clickhouse_ingest_topic_staging_partition = list(filter(lambda p: p["topic"] == "clickhouse_ingest_topic_staging", partitions)).pop()
        if clickhouse_ingest_topic_staging_partition is None:
            return False
        print("Get clickhouse ingest topic staging partition")
        ch_client = clickhouse_connect.get_client(host=settings.clickhouse_host, database=settings.clickhouse_db,
                                                  port=settings.clickhouse_port, username=settings.clickhouse_username, password=settings.clickhouse_password)
        result_clickhouse = ch_client.query("""
                            SELECT
                                assignments.topic, assignments.current_offset
                            FROM system.kafka_consumers
                            WHERE database = 'report'
                              AND table = 'clickhouse_ingest_topic_staging'
                        """)
        result_clickhouse = list(result_clickhouse.named_results()).pop()
        if result_clickhouse is None:
            return False
        print(result_clickhouse)
        clickhouse_kafka_current_offset = result_clickhouse["assignments.current_offset"].pop()
        current_offset = clickhouse_ingest_topic_staging_partition["currentOffset"]
        end_offset = clickhouse_ingest_topic_staging_partition["endOffset"]
        print("Current offset", current_offset)
        print("End offset", end_offset)
        print("clickhouse_kafka_current_offset:", clickhouse_kafka_current_offset)
        is_kafka_sync_success = clickhouse_ingest_topic_staging_partition["currentOffset"] == clickhouse_ingest_topic_staging_partition["endOffset"]
        print("is_kafka_sync_success: {}".format(is_kafka_sync_success))
        if clickhouse_kafka_current_offset == -1001:
            print("Clickhouse inconsistencies. Use kafa info")
            return is_kafka_sync_success
        is_clickhouse_sync_success = clickhouse_kafka_current_offset == clickhouse_ingest_topic_staging_partition["endOffset"]
        is_sync_success = is_kafka_sync_success and is_clickhouse_sync_success
        print("is_sync_success: {}".format(is_sync_success))
        return is_sync_success
    except Exception as e:
        print("Exception:", e)
        return False
with DAG(
    dag_id="week_n_month_revenue_calc",
    schedule_interval=None,
    default_args=default_args,
) as dag:
    validate_data_sync_to_clickhouse = PythonSensor(
        task_id="validate_data_sync_to_clickhouse",
        python_callable=validate_data_sync_to_clickhouse
    )
    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="dbt-runtime:1.0.0",
        command="dbt run --select mart_weekly_report",
        mounts=[
            Mount(
                source="/Users/giangnt/code/py-apps/py-spark/dbt/ecm_dbt_proj",
                target="/app",
                type="bind"
            )
        ],
        working_dir="/app",
        mount_tmp_dir=False,
        auto_remove=True

    )