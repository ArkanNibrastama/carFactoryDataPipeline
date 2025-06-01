from pyspark.sql import SparkSession
from datetime import date, timedelta
import sys

yesterday = (date.today() - timedelta(days = 1)).strftime('%Y%m%d')
date = sys.argv[1] if (len(sys.argv) > 1) or (len(sys.argv) == 0) else yesterday
source_path = f"s3a://datalake/sensor_data/battery_assembly_machine_sensor/{date}/"

spark = SparkSession.builder \
    .appName("UpsertDataToBatteryAssemblyMachineIceberg") \
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
MERGE INTO nessie.nessie_catalog.battery_assembly_machine_sensor AS target
USING staging_data AS source
ON target.timestamp = source.timestamp AND target.machine_id = source.machine_id
WHEN MATCHED THEN 
    UPDATE SET 
      welding_current_amps = source.welding_current_amps,
      welding_voltage_volts = source.welding_voltage_volts,
      electrode_temperature_celsius = source.electrode_temperature_celsius,
      cooling_system_temperature_celsius = source.cooling_system_temperature_celsius,
      alignment_accuracy_mm = source.alignment_accuracy_mm,
      vacuum_pressure_kpa = source.vacuum_pressure_kpa,
      cell_voltage_uniformity_ratio = source.cell_voltage_uniformity_ratio,
      insulation_resistance_mohm = source.insulation_resistance_mohm,
      leakage_current_ma = source.leakage_current_ma,
      operation_time_hours = source.operation_time_hours,
      batteries_produced = source.batteries_produced,
      electrode_wear_percent = source.electrode_wear_percent,
WHEN NOT MATCHED THEN 
    INSERT (
      timestamp,
      machine_id,
      machine_type,
      welding_current_amps,
      welding_voltage_volts,
      electrode_temperature_celsius,
      cooling_system_temperature_celsius,
      alignment_accuracy_mm,
      vacuum_pressure_kpa,
      cell_voltage_uniformity_ratio,
      insulation_resistance_mohm,
      leakage_current_ma,
      operation_time_hours,
      batteries_produced,
      electrode_wear_percent,
    )
    VALUES (
      source.timestamp,
      source.machine_id,
      source.machine_type,
      source.welding_current_amps,
      source.welding_voltage_volts,
      source.electrode_temperature_celsius,
      source.cooling_system_temperature_celsius,
      source.alignment_accuracy_mm,
      source.vacuum_pressure_kpa,
      source.cell_voltage_uniformity_ratio,
      source.insulation_resistance_mohm,
      source.leakage_current_ma,
      source.operation_time_hours,
      source.batteries_produced,
      source.electrode_wear_percent,
    );
""")

spark.stop()
