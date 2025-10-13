import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
import joblib
from datetime import datetime
import pandas as pd
import numpy as np
import s3fs 

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define the schema for JSON data
input_schema = JsonRowDeserializationSchema.builder() \
    .type_info(type_info=Types.ROW_NAMED(
        ["timestamp", "machine_id", "machine_type", "readings"],  # Top-level fields
        [Types.STRING(), Types.STRING(), Types.STRING(), 
         Types.ROW_NAMED(  # Nested structure for readings
             ["welding_current_amps","welding_voltage_volts","electrode_temperature_celsius",
              "cooling_system_temperature_celsius","alignment_accuracy_mm","vacuum_pressure_kpa",
              "cell_voltage_uniformity_ratio","insulation_resistance_mohm","leakage_current_ma",
              "operation_time_hours","batteries_produced","electrode_wear_percent"],
             [Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
              Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(),
              Types.FLOAT(), Types.FLOAT(), Types.INT(), Types.FLOAT()]
         )]
    )).build()

output_schema = Types.ROW([
                    Types.STRING(), 
                    Types.STRING(), 
                    Types.STRING(), 
                    Types.FLOAT(), 
                    Types.STRING()
                ]) 


def preprocessing(data):

    data_point = {
        'welding_current_amps': data['welding_current_amps'],
        'welding_voltage_volts': data['welding_voltage_volts'],
        'electrode_temperature_celsius': data['electrode_temperature_celsius'],
        'cooling_system_temperature_celsius': data['cooling_system_temperature_celsius'],
        'alignment_accuracy_mm': data['alignment_accuracy_mm'],
        'vacuum_pressure_kpa': data['vacuum_pressure_kpa'],
        'cell_voltage_uniformity_ratio': data['cell_voltage_uniformity_ratio'],
        'insulation_resistance_mohm': data['insulation_resistance_mohm'],
        'leakage_current_ma': data['leakage_current_ma'],
        'electrode_wear_percent': data['electrode_wear_percent'],
        'operation_time_hours': data['operation_time_hours'],
        'batteries_produced': data['batteries_produced'],
            
        # Add default values for additional required features
        'time_since_maintenance': 0,
        'hour_of_day': datetime.now().hour,
        'day_of_week': datetime.now().weekday()
    }

    features = pd.DataFrame([data_point])

    features['current_voltage_ratio'] = features['welding_current_amps'] / np.maximum(features['welding_voltage_volts'], 0.1)
    features['electrode_wear_per_hour'] = features['electrode_wear_percent'] / np.maximum(features['operation_time_hours'], 0.1)
    features['temp_diff'] = features['electrode_temperature_celsius'] - features['cooling_system_temperature_celsius']
    features['electrical_stability'] = features['cell_voltage_uniformity_ratio'] * features['insulation_resistance_mohm'] / np.maximum(features['leakage_current_ma'], 0.001)
         
    column_list = ['welding_current_amps', 'welding_voltage_volts', 'electrode_temperature_celsius', 
                    'cooling_system_temperature_celsius', 'alignment_accuracy_mm', 'vacuum_pressure_kpa', 
                    'cell_voltage_uniformity_ratio', 'insulation_resistance_mohm', 'leakage_current_ma', 
                    'electrode_wear_percent', 'operation_time_hours', 'batteries_produced', 'time_since_maintenance', 
                    'hour_of_day', 'day_of_week', 'current_voltage_ratio', 'electrode_wear_per_hour', 'temp_diff', 
                    'electrical_stability', 'welding_current_amps_prev', 'welding_voltage_volts_prev', 
                    'electrode_temperature_celsius_prev', 'cooling_system_temperature_celsius_prev', 'alignment_accuracy_mm_prev', 
                    'vacuum_pressure_kpa_prev', 'cell_voltage_uniformity_ratio_prev', 'insulation_resistance_mohm_prev', 
                    'leakage_current_ma_prev', 'electrode_wear_percent_prev', 'welding_current_amps_change', 
                    'welding_voltage_volts_change', 'electrode_temperature_celsius_change', 'cooling_system_temperature_celsius_change', 
                    'alignment_accuracy_mm_change', 'vacuum_pressure_kpa_change', 'cell_voltage_uniformity_ratio_change', 
                    'insulation_resistance_mohm_change', 'leakage_current_ma_change', 'electrode_wear_percent_change'
                ]

     # Align features with the model's expected format
    required_features = column_list
    missing_features = set(required_features) - set(features.columns)
        
    # Initialize missing features with zero or appropriate values
    for feature in missing_features:
        if '_prev' in feature:
            base_feature = feature.replace('_prev', '')
            if base_feature in features.columns:
                features[feature] = features[base_feature]
            else:
                features[feature] = 0
        elif '_change' in feature:
            features[feature] = 0
        else:
            features[feature] = 0
        
    # Select features in the same order as during training
    features_for_prediction = features[required_features]

    return features_for_prediction

def load_model_from_minio():
    # Create s3fs filesystem with MinIO credentials and endpoint
    fs = s3fs.S3FileSystem(
        key='minioadmin',
        secret='minioadmin',
        client_kwargs={'endpoint_url': 'http://minio:9001'}
    )

    # Path to the model file in the MinIO bucket
    model_path = 'model/battery_failure_prediction_model.pkl'

    # Open the file using s3fs and load it with joblib
    with fs.open(model_path, 'rb') as f:
        model = joblib.load(f)

    return model

# Transformation function
def transform_data(row, model):
    try:

        features = preprocessing(row["readings"])
        machine_id = row["machine_id"]
        machine_type = row["machine_type"]

        failure_prob = model.predict_proba(features)[0][1]

        if failure_prob < 0.3:
            risk_level = "LOW"
        elif failure_prob < 0.7:
            risk_level = "MEDIUM"
        else:
            risk_level = "HIGH"
        
        return Row(
            datetime.now().isoformat(),
            machine_id,
            machine_type,
            round(failure_prob, 3),
            risk_level
        )

    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        print(f"Error transforming data: {e}")
        return None

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Kafka connector dependencies
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.2.0-1.18.jar")
    env.add_jars("file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar")
    env.add_jars("file:///opt/flink/lib/clickhouse-jdbc-0.7.2.jar")

    # Kafka consumer configuration
    kafka_consumer = FlinkKafkaConsumer(
        topics="battery_assembly_machine_sensor",
        deserialization_schema=input_schema,
        properties={
            "bootstrap.servers": "kafka:9092",
        },
    )

    jdbc_connection_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_url("jdbc:clickhouse://clickhouse:8123/stream_data_warehouse") \
        .with_driver_name("com.clickhouse.jdbc.ClickHouseDriver") \
        .with_user_name("admin") \
        .with_password("admin") \
        .build()
    
    jdbc_execution_options = JdbcExecutionOptions.builder() \
        .with_batch_interval_ms(1000) \
        .with_batch_size(200) \
        .with_max_retries(5) \
        .build()
    
    jdbc_sink = JdbcSink.sink(
        # Adjust this SQL to match your ClickHouse table structure
        "INSERT INTO sensor_data (prediction_timestamp, machine_id, machine_type, failure_probability, risk_level) VALUES (?, ?, ?, ?, ?)",
        output_schema,
        jdbc_connection_options,
        jdbc_execution_options
    )

    # Build the pipeline
    stream = env.add_source(kafka_consumer)
    model = load_model_from_minio()
    transformed_stream = stream.map(
        lambda sensor_data: transform_data(sensor_data, model),
        output_type=output_schema
    )
    transformed_stream.filter(lambda row: row is not None).add_sink(jdbc_sink)

    # Execute the pipeline
    env.execute("battery assembly machine to clickhouse job")

if __name__ == "__main__":
    main()