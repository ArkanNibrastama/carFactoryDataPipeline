from pyspark.sql import SparkSession
from datetime import date, timedelta
from pyspark.sql.window import Window
from pyspark.sql.functions import avg
import sys

yesterday = (date.today() - timedelta(days = 1)).strftime('%Y%m%d')
date = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1].strip() else yesterday
source_path = f"s3a://datalake/sensor_data/injection_molding_machine_sensor/{date}/"

spark = SparkSession.builder \
    .appName("UpsertDataToInjectionMoldingMachineIceberg") \
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

windowMovAvgBarrelTemp = Window.partitionBy('machine_id').\
                                orderBy('timestamp').\
                                rowsBetween(-6, 0)

df = df.withColumn(
    "moving_avg_barrel_temperature",
    avg('barrel_temperature_celsius').over(windowMovAvgBarrelTemp)
)

df.createOrReplaceTempView("staging_data")

spark.sql("""
MERGE INTO nessie.nessie_catalog.injection_molding_machine_sensor AS target
USING staging_data AS source
ON target.timestamp = source.timestamp AND target.machine_id = source.machine_id
WHEN MATCHED THEN 
    UPDATE SET 
      barrel_temperature_celsius = source.barrel_temperature_celsius,
      moving_avg_barrel_temperature = source.moving_avg_barrel_temperature,
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
      mold_wear_percent = source.mold_wear_percent
WHEN NOT MATCHED THEN 
    INSERT (
      timestamp,
      machine_id,
      machine_type,
      barrel_temperature_celsius,
      moving_avg_barrel_temperature,
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
      mold_wear_percent
    )
    VALUES (
      source.timestamp,
      source.machine_id,
      source.machine_type,
      source.barrel_temperature_celsius,
      source.moving_avg_barrel_temperature,
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
      source.mold_wear_percent
    );
""")

spark.stop()
