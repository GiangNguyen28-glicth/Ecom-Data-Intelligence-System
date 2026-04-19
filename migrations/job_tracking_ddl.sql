CREATE DATABASE report

CREATE TYPE job_status_enum AS ENUM (
  'IDLE',
  'RUNNING',
  'COMPLETED',
  'ERROR'
);

CREATE TYPE calc_daily_revenue_process_enum AS ENUM (
  'INIT_TRACKING_JOB',
  'TRANSFER_DATA_DAILY_SNAPSHOT_TO_ICEBERG',
  'CALC_DALY_REVENUE',
  'TRANSFER_DAILY_REVENUE_DATA_TO_KAFKA',
  'CLEAR_OLDER_THAN_N_MONTHS'
);

CREATE TABLE IF NOT EXISTS jobs (
  id VARCHAR(50) PRIMARY KEY,
  name VARCHAR(255),
  run_id VARCHAR(255),
  status job_status_enum,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  process_at calc_daily_revenue_process_enum,
  metadata JSONB
);