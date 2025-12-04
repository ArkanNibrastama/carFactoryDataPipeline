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


-- create a projection on how many failure on each machine type?
ALTER TABLE stream_data_warehouse.sensor_data
ADD PROJECTION risk_level_by_machine_today(
    SELECT toDate(prediction_timestamp) as date,
            machine_type,
            machine_id,
            risk_level,
            count() as risk_count
    GROUP BY date,
            machine_type,
            machine_id,
            risk_level
    ORDER BY date,
            machine_type,
            machine_id,
            risk_level
);


-- Materialize the projection (for existing data)
-- just in case there is data that inserted before the projection made 
ALTER TABLE stream_data_warehouse.sensor_data
MATERIALIZE PROJECTION risk_level_by_machine_today;

-- Optimize to merge parts
OPTIMIZE TABLE stream_data_warehouse.sensor_data FINAL;