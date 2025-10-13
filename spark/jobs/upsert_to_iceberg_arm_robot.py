from pyspark.sql import SparkSession
from datetime import date, timedelta
import sys

yesterday = (date.today() - timedelta(days = 1)).strftime('%Y%m%d')
date = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1].strip() else yesterday
source_path = f"s3a://datalake/sensor_data/arm_robot_sensor/{date}/"


spark = SparkSession.builder \
    .appName("UpsertDataToArmRobotIceberg") \
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
MERGE INTO nessie.nessie_catalog.arm_robot_sensor AS target
USING staging_data AS source
ON target.timestamp = source.timestamp AND target.machine_id = source.machine_id
WHEN MATCHED THEN 
    UPDATE SET 
      servo_motor_rpm = source.servo_motor_rpm,
      hydraulic_pressure_psi = source.hydraulic_pressure_psi,
      temperature_celsius = source.temperature_celsius,
      vibration_mm_per_s = source.vibration_mm_per_s,
      operation_time_hours = source.operation_time_hours,
      cycle_count = source.cycle_count,
      joint_wear_percent = source.joint_wear_percent
WHEN NOT MATCHED THEN 
    INSERT (
      timestamp,
      machine_id,
      machine_type,
      servo_motor_rpm,
      hydraulic_pressure_psi,
      temperature_celsius,
      vibration_mm_per_s,
      operation_time_hours,
      cycle_count,
      joint_wear_percent
    )
    VALUES (
      source.timestamp,
      source.machine_id,
      source.machine_type,
      source.servo_motor_rpm,
      source.hydraulic_pressure_psi,
      source.temperature_celsius,
      source.vibration_mm_per_s,
      source.operation_time_hours,
      source.cycle_count,
      source.joint_wear_percent
    );
""")

spark.stop()
