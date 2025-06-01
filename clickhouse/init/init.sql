CREATE DATABASE IF NOT EXISTS stream_data_warehouse;

CREATE TABLE IF NOT EXISTS stream_data_warehouse.sensor_data
(
    prediction_timestamp VARCHAR,
    machine_id VARCHAR,
    machine_type VARCHAR,
    failure_probability FLOAT,
    risk_level VARCHAR
)
ENGINE = MergeTree()
ORDER BY (prediction_timestamp, machine_type, machine_id);