from pyspark.sql import SparkSession
from datetime import date, timedelta
import sys

yesterday = (date.today() - timedelta(days = 1)).strftime('%Y%m%d')
date = sys.argv[1] if (len(sys.argv) > 1) or (len(sys.argv) == 0) else yesterday
source_path = f"s3a://datalake/sensor_data/injection_molding_machine_sensor/{date}/"

spark = SparkSession.builder \
    .appName("UpsertDataToInjectionMoldingMachineIceberg") \
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
MERGE INTO nessie.nessie_catalog.injection_molding_machine_sensor AS target
USING staging_data AS source
ON target.timestamp = source.timestamp AND target.machine_id = source.machine_id
WHEN MATCHED THEN 
    UPDATE SET 
      barrel_temperature_celsius = source.barrel_temperature_celsius,
      mold_temperature_celsius = source.mold_temperature_celsius,
      injection_pressure_bar = source.injection_pressure_bar,
      holding_pressure_bar = source.holding_pressure_bar,
      injection_speed_mm_per_s = source.injection_speed_mm_per_s,
      cooling_time_s = source.cooling_time_s,
      cycle_time_s = source.cycle_time_s,
      material_viscosity_pa_s = source.material_viscosity_pa_s,
      mold_clamping_force_tons = source.mold_clamping_force_tons,
      operation_time_hours = source.operation_time_hours,
      parts_produced = source.parts_produced,
      mold_wear_percent = source.mold_wear_percent,
WHEN NOT MATCHED THEN 
    INSERT (
      timestamp,
      machine_id,
      machine_type,
      barrel_temperature_celsius,
      mold_temperature_celsius,
      injection_pressure_bar,
      holding_pressure_bar,
      injection_speed_mm_per_s,
      cooling_time_s,
      cycle_time_s,
      material_viscosity_pa_s,
      mold_clamping_force_tons,
      operation_time_hours,
      parts_produced,
      mold_wear_percent,
    )
    VALUES (
      source.timestamp,
      source.machine_id,
      source.machine_type,
      source.barrel_temperature_celsius,
      source.mold_temperature_celsius,
      source.injection_pressure_bar,
      source.holding_pressure_bar,
      source.injection_speed_mm_per_s,
      source.cooling_time_s,
      source.cycle_time_s,
      source.material_viscosity_pa_s,
      source.mold_clamping_force_tons,
      source.operation_time_hours,
      source.parts_produced,
      source.mold_wear_percent,
    );
""")

spark.stop()
