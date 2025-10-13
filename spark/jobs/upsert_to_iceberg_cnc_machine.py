from pyspark.sql import SparkSession
from datetime import date, timedelta
import sys

yesterday = (date.today() - timedelta(days = 1)).strftime('%Y%m%d')
date = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1].strip() else yesterday
source_path = f"s3a://datalake/sensor_data/cnc_machine_sensor/{date}/"

spark = SparkSession.builder \
    .appName("UpsertDataToCNCMachineIceberg") \
    .getOrCreate()

print(f"Reading from: {source_path}")
print(f"Date argument: {date}")

try:
    df = spark.read.parquet(source_path)
    if df.count() == 0:
        print(f"No data found in {source_path}")
        spark.stop()
        sys.exit(0)
except Exception as e:
    print(f"Error reading from {source_path}: {e}")
    spark.stop()
    sys.exit(1)

print(f"Number of records: {df.count()}")
df.createOrReplaceTempView("staging_data")

spark.sql("""
MERGE INTO nessie.nessie_catalog.cnc_machine_sensor AS target
USING staging_data AS source
ON target.timestamp = source.timestamp AND target.machine_id = source.machine_id
WHEN MATCHED THEN 
    UPDATE SET 
      spindle_speed_rpm = source.spindle_speed_rpm,
      cutting_force_n = source.cutting_force_n,
      motor_temperature_celsius = source.motor_temperature_celsius,
      coolant_temperature_celsius = source.coolant_temperature_celsius,
      coolant_flow_rate_l_min = source.coolant_flow_rate_l_min,
      vibration_mm_per_s = source.vibration_mm_per_s,
      tool_wear_percent = source.tool_wear_percent,
      operation_time_hours = source.operation_time_hours,
      parts_produced = source.parts_produced
WHEN NOT MATCHED THEN 
    INSERT (
      timestamp,
      machine_id,
      machine_type,
      spindle_speed_rpm,
      cutting_force_n,
      motor_temperature_celsius,
      coolant_temperature_celsius,
      coolant_flow_rate_l_min,
      vibration_mm_per_s,
      tool_wear_percent,
      operation_time_hours,
      parts_produced
    )
    VALUES (
      source.timestamp,
      source.machine_id,
      source.machine_type,
      source.spindle_speed_rpm,
      source.cutting_force_n,
      source.motor_temperature_celsius,
      source.coolant_temperature_celsius,
      source.coolant_flow_rate_l_min,
      source.vibration_mm_per_s,
      source.tool_wear_percent,
      source.operation_time_hours,
      source.parts_produced
    );
""")

spark.stop()
