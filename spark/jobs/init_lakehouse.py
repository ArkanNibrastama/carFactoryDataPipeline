from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


spark = SparkSession.builder \
     .appName("InitLakehouseSchema") \
     .config('spark.jars', '/opt/spark/jars/hadoop-aws-3.3.4.jar,'
            '/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,'
            '/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,'
            '/opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.76.6.jar') \
    .config('spark.sql.extensions', 
            'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
            'org.projectnessie.spark.extensions.NessieSparkSessionExtensions') \
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
    .getOrCreate()


spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.nessie_catalog")

# Create a DataFrame
empty_rdd = spark.sparkContext.emptyRDD()

columns_arm_robot_machine = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("servo_motor_rpm", DoubleType(), True),
    StructField("hydraulic_pressure_psi", DoubleType(), True),
    StructField("temperature_celsius", DoubleType(), True),
    StructField("moving_avg_temperature_celsius", DoubleType(), True),
    StructField("vibration_mm_per_s", DoubleType(), True),
    StructField("operation_time_hours", DoubleType(), True),
    StructField("cycle_count", IntegerType(), True),
    StructField("joint_wear_percent", DoubleType(), True)
])

columns_cnc_machine = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("spindle_speed_rpm", DoubleType(), True),
    StructField("cutting_force_n", DoubleType(), True),
    StructField("motor_temperature_celsius", DoubleType(), True),
    StructField("moving_avg_motor_temperature", DoubleType(), True),
    StructField("coolant_temperature_celsius", DoubleType(), True),
    StructField("coolant_flow_rate_l_min", DoubleType(), True),
    StructField("vibration_mm_per_s", DoubleType(), True),
    StructField("tool_wear_percent", DoubleType(), True),
    StructField("operation_time_hours", DoubleType(), True),
    StructField("parts_produced", IntegerType(), True),
])

columns_injection_molding_machine = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("barrel_temperature_celsius", DoubleType(), True),
    StructField("moving_avg_barrel_temperature", DoubleType(), True),
    StructField("mold_temperature_celsius", DoubleType(), True),
    StructField("injection_pressure_bar", DoubleType(), True),
    StructField("holding_pressure_bar", DoubleType(), True),
    StructField("injection_speed_mm_per_s", DoubleType(), True),
    StructField("cooling_time_s", DoubleType(), True),
    StructField("cycle_time_s", DoubleType(), True),
    StructField("material_viscosity_pa_s", DoubleType(), True),
    StructField("mold_clamping_force_tons", DoubleType(), True),
    StructField("operation_time_hours", DoubleType(), True),
    StructField("parts_produced", IntegerType(), True),
    StructField("mold_wear_percent", DoubleType(), True),
])

columns_battery_assembly_machine = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("welding_current_amps", DoubleType(), True),
    StructField("welding_voltage_volts", DoubleType(), True),
    StructField("electrode_temperature_celsius", DoubleType(), True),
    StructField("cooling_system_temperature_celsius", DoubleType(), True),
    StructField("moving_avg_cooling_system_temperature", DoubleType(), True),
    StructField("alignment_accuracy_mm", DoubleType(), True),
    StructField("vacuum_pressure_kpa", DoubleType(), True),
    StructField("cell_voltage_uniformity_ratio", DoubleType(), True),
    StructField("insulation_resistance_mohm", DoubleType(), True),
    StructField("leakage_current_ma", DoubleType(), True),
    StructField("operation_time_hours", DoubleType(), True),
    StructField("batteries_produced", IntegerType(), True),
    StructField("electrode_wear_percent", DoubleType(), True),
])


df_arm_robot_machine = spark.createDataFrame(empty_rdd, columns_arm_robot_machine)
df_cnc_machine = spark.createDataFrame(empty_rdd, columns_cnc_machine)
df_injection_molding_machine = spark.createDataFrame(empty_rdd, columns_injection_molding_machine)
df_battery_assembly_machine = spark.createDataFrame(empty_rdd, columns_battery_assembly_machine)

df_arm_robot_machine.write.format("iceberg").mode("overwrite") \
                    .saveAsTable("nessie.nessie_catalog.arm_robot_sensor")
df_cnc_machine.write.format("iceberg").mode("overwrite") \
                    .saveAsTable("nessie.nessie_catalog.cnc_machine_sensor")
df_injection_molding_machine.write.format("iceberg").mode("overwrite") \
                    .saveAsTable("nessie.nessie_catalog.injection_molding_machine_sensor")
df_battery_assembly_machine.write.format("iceberg").mode("overwrite") \
                    .saveAsTable("nessie.nessie_catalog.battery_assembly_machine_sensor")

spark.stop()
