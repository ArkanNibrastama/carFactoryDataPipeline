import json
import logging
import os
import time
from datetime import datetime
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Kafka configuration
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Generate dummy sensor data
def generate_sensor_data():
    return {
            "timestamp": datetime.now().isoformat(),
            "machine_id": "1",
            "machine_type": "ROBOTIC_ARM",
            "readings": {
                "servo_motor_rpm": 0.18,
                "hydraulic_pressure_psi": 3.4,
                "temperature_celsius": 20.1,
                "vibration_mm_per_s": 12.12,
                "operation_time_hours": 1.5,
                "cycle_count": 1,
                "joint_wear_percent": 2.4
            }
        }

# Publish data to Kafka
try:
    while True:
        data = generate_sensor_data()
        producer.send(KAFKA_TOPIC, value=data)
        logging.info(f"Produced: {data}")
        print(data)
        time.sleep(5)  # Send data every 5 seconds
except Exception as e:
    logging.error(f"Error in producer: {e}")
    print(f"Error in producer: {e}")
finally:
    producer.close()