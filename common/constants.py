LAST_STATE_PRODUCT_ITEM_TABLE = 'ecm_catalog.report.last_state_product_item'
LAST_STATE_PRODUCT_ITEM_STAGING_TABLE = 'ecm_catalog.report.last_state_product_item_staging'
PRODUCT_ITEM_DAILY_TABLE = 'ecm_catalog.report.product_item_daily'
PRODUCT_ITEM_DAILY_STAGING_TABLE = 'ecm_catalog.report.product_item_daily_staging'
PRODUCT_ITEM_DAILY_REVENUE_TABLE = 'ecm_catalog.report.product_item_daily_revenue'
PRODUCT_ITEM_DAILY_REVENUE_STAGING_TABLE = 'ecm_catalog.report.product_item_daily_revenue_staging'
PARSED_BUCKET = 'parsed-data'
RAW_BUCKET = 'raw-data'
PARSED_BUCKET_STAGING = 'parsed-data-staging'
JOB_STATUS = {
    "IDLE": 'IDLE',
    "RUNNING": 'RUNNING',
    "COMPLETED": 'COMPLETED',
    "ERROR": 'ERROR'
}
CALC_DAILY_REVENUE_PROCESS = {
    "INIT_TRACKING_JOB": 'INIT_TRACKING_JOB',
    "TRANSFER_DATA_DAILY_SNAPSHOT_TO_ICEBERG": "TRANSFER_DATA_DAILY_SNAPSHOT_TO_ICEBERG",
    "TRANSFER_DATA_DAILY_SNAPSHOT_TO_LATEST": "TRANSFER_DATA_DAILY_SNAPSHOT_TO_LATEST",
    "CALC_DALY_REVENUE": "CALC_DALY_REVENUE",
    "TRANSFER_DAILY_REVENUE_DATA_TO_KAFKA": "TRANSFER_DAILY_REVENUE_DATA_TO_KAFKA",
    "CLEAR_OLDER_THAN_N_MONTHS": "CLEAR_OLDER_THAN_N_MONTHS",
}
POSTGRESQL_CONN = 'postgresql_default'
REPORT_TABLE = {
    "JOBS": "jobs"
}

SPARK_AIRFLOW_DEFAULT_CONFIG = {
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "1",
    "spark.cores.max": "1",
    "spark.driver.host": "host.docker.internal",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.task.maxFailures": "4",
    "spark.yarn.maxAppAttempts": "1",
    "spark.driver.port": "7010",
    "spark.blockManager.port": "7011",
    "spark.rpc.askTimeout": "120s",
    # "spark.eventLog.enabled": "true",
    # "spark.eventLog.dir": "file:/opt/spark-events"
}
