from pyspark.sql import SparkSession
from datetime import date, timedelta
import sys

yesterday = (date.today() - timedelta(days = 1)).strftime('%Y%m%d')
date = sys.argv[1] if (len(sys.argv) > 1) or (len(sys.argv) == 0) else yesterday
source_path = f"s3a://datalake/sensor_data/cnc_machine_sensor/{date}/"

spark = SparkSession.builder \
    .appName("UpsertDataToCNCMachineIceberg") \
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
