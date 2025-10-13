from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import avg
import sys

yesterday = (date.today() - timedelta(days = 1)).strftime('%Y%m%d')
date = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1].strip() else yesterday
source_path = f"s3a://datalake/sensor_data/battery_assembly_machine_sensor/{date}/"

spark = SparkSession.builder \
    .appName("UpsertDataToBatteryAssemblyMachineIceberg") \
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

windowMovAvgCoolingSystemTemp = Window.partitionBy('machine_id').\
                                       orderBy('timestamp').\
                                       rowsBetween(-6, 0)

df = df.withColumn(
    "moving_avg_cooling_system_temperature",
    avg('cooling_system_temperature_celsius').over(windowMovAvgCoolingSystemTemp)
)

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
      moving_avg_cooling_system_temperature = source.moving_avg_cooling_system_temperature,
      alignment_accuracy_mm = source.alignment_accuracy_mm,
      vacuum_pressure_kpa = source.vacuum_pressure_kpa,
      cell_voltage_uniformity_ratio = source.cell_voltage_uniformity_ratio,
      insulation_resistance_mohm = source.insulation_resistance_mohm,
      leakage_current_ma = source.leakage_current_ma,
      operation_time_hours = source.operation_time_hours,
      batteries_produced = source.batteries_produced,
      electrode_wear_percent = source.electrode_wear_percent
WHEN NOT MATCHED THEN 
    INSERT (
      timestamp,
      machine_id,
      machine_type,
      welding_current_amps,
      welding_voltage_volts,
      electrode_temperature_celsius,
      cooling_system_temperature_celsius,
      moving_avg_cooling_system_temperature,
      alignment_accuracy_mm,
      vacuum_pressure_kpa,
      cell_voltage_uniformity_ratio,
      insulation_resistance_mohm,
      leakage_current_ma,
      operation_time_hours,
      batteries_produced,
      electrode_wear_percent
    )
    VALUES (
      source.timestamp,
      source.machine_id,
      source.machine_type,
      source.welding_current_amps,
      source.welding_voltage_volts,
      source.electrode_temperature_celsius,
      source.cooling_system_temperature_celsius,
      source.moving_avg_cooling_system_temperature,
      source.alignment_accuracy_mm,
      source.vacuum_pressure_kpa,
      source.cell_voltage_uniformity_ratio,
      source.insulation_resistance_mohm,
      source.leakage_current_ma,
      source.operation_time_hours,
      source.batteries_produced,
      source.electrode_wear_percent
    );
""")

spark.stop()
