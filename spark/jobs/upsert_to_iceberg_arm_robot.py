from pyspark.sql import SparkSession
from datetime import date, timedelta
import sys

yesterday = (date.today() - timedelta(days = 1)).strftime('%Y%m%d')
date = sys.argv[1] if (len(sys.argv) > 1) or (len(sys.argv) == 0) else yesterday
source_path = f"s3a://datalake/sensor_data/arm_robot_sensor/{date}/"

spark = SparkSession.builder \
    .appName("UpsertDataToArmRobotIceberg") \
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v2") \
    .config("spark.sql.catalog.nessie.ref", "main") \
    .config("spark.sql.catalog.nessie.authentication.type", "NONE") \
    .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9001") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

df = spark.read.option("header", "true").format("parquet").load(source_path)
df.show()
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
