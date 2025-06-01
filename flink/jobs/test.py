import os
import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import Row
import joblib
from datetime import datetime
import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Define the schema for JSON data
input_schema = JsonRowDeserializationSchema.builder() \
    .type_info(type_info=Types.ROW_NAMED(
        ["timestamp", "machine_id", "machine_type", "readings"],  # Top-level fields
        [Types.STRING(), Types.STRING(), Types.STRING(), 
         Types.ROW_NAMED(  # Nested structure for readings
             ["servo_motor_rpm", "hydraulic_pressure_psi", "temperature_celsius", 
              "vibration_mm_per_s", "operation_time_hours", "cycle_count", "joint_wear_percent"],
             [Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), 
              Types.FLOAT(), Types.FLOAT(), Types.INT(), Types.FLOAT()]
         )]
    )).build()

output_schema = JsonRowSerializationSchema.builder() \
    .with_type_info(type_info=Types.ROW_NAMED(
        ["prediction_timestamp", "machine_id", "machine_type", "failure_probability", "risk_level"],  # Field names
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING()]  # Field types
    )) \
    .build() 


def preprocessing(data):

    data_point = {
        'servo_motor_rpm': data['servo_motor_rpm'],
        'hydraulic_pressure_psi': data['hydraulic_pressure_psi'],
        'temperature_celsius': data['temperature_celsius'],
        'vibration_mm_per_s': data['vibration_mm_per_s'],
        'operation_time_hours': data['operation_time_hours'],
        'cycle_count': data['cycle_count'],
        'joint_wear_percent': data['joint_wear_percent'],
            
        # Add default values for additional required features
        'time_since_maintenance': 0,
        'hour_of_day': datetime.now().hour,
        'day_of_week': datetime.now().weekday()
    }

    features = pd.DataFrame([data_point])

    features['pressure_temp_ratio'] = features['hydraulic_pressure_psi'] / features['temperature_celsius']
    features['wear_per_hour'] = features['joint_wear_percent'] / np.maximum(features['operation_time_hours'], 0.1)
    features['rpm_vibration_ratio'] = features['servo_motor_rpm'] / np.maximum(features['vibration_mm_per_s'], 0.1)
        
    column_list = ['servo_motor_rpm', 'hydraulic_pressure_psi', 'temperature_celsius', 
                      'vibration_mm_per_s', 'operation_time_hours', 'cycle_count', 'joint_wear_percent', 
                      'time_since_maintenance', 'hour_of_day', 'day_of_week', 'pressure_temp_ratio', 
                      'wear_per_hour', 'rpm_vibration_ratio', 'servo_motor_rpm_prev', 
                      'hydraulic_pressure_psi_prev', 'temperature_celsius_prev', 'vibration_mm_per_s_prev', 
                      'joint_wear_percent_prev', 'servo_motor_rpm_change', 'hydraulic_pressure_psi_change', 
                      'temperature_celsius_change', 'vibration_mm_per_s_change', 'joint_wear_percent_change'
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

# Transformation function
def transform_data(row):
    try:

        print(f"input: {row}")

        # load model
        model = joblib.load("../models/robotic_arm_failure_prediction_model.pkl")

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
            prediction_timestamp= datetime.now().isoformat(),
            machine_id= machine_id,
            machine_type= machine_type,
            failure_probability= round(failure_prob, 3),
            risk_level= risk_level
        )
        
        # ========================================================================
        # machine_id = row["machine_id"]
        # readings = row["readings"]
        # temperature_celsius = readings["temperature_celsius"]
        # timestamp = row["timestamp"]

        # # Convert temperature to Fahrenheit
        # temperature_fahrenheit = (temperature_celsius * 9 / 5) + 32

        # print("transformed")

        # # Add location
        # return Row(
        #     machine_id = machine_id,
        #     location = "Room 101",
        #     temperature_fahreinheit = temperature_fahrenheit,
        #     timestamp = timestamp
        # )
        # ========================================================================

    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        print(f"Error transforming data: {e}")
        return None

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Kafka connector dependencies
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.2.0-1.18.jar")
    # env.add_jars("file:///opt/flink/lib/kafka-clients-3.4.0.jar")


    # Kafka consumer configuration
    kafka_consumer = FlinkKafkaConsumer(
        topics="arm_robot_machine_sensor",
        deserialization_schema=input_schema,
        properties={
            "bootstrap.servers": "kafka:9092",
            "group.id": "flink-consumer-group",
        },
    )

    # Kafka producer configuration
    kafka_producer = FlinkKafkaProducer(
        topic="transformed_arm_robot_machine_sensor",
        serialization_schema=output_schema,
        producer_config={
            "bootstrap.servers": "kafka:9092",
        },
    )

    # Build the pipeline
    stream = env.add_source(kafka_consumer)
    transformed_stream = stream.map(transform_data, output_type=Types.ROW_NAMED(
        ["prediction_timestamp", "machine_id", "machine_type", "failure_probability", "risk_level"],  # Field names
        [Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING()]  # Field types
    ))
    transformed_stream.filter(lambda row: row is not None).add_sink(kafka_producer)

    # Execute the pipeline
    env.execute("test")

if __name__ == "__main__":
    main()