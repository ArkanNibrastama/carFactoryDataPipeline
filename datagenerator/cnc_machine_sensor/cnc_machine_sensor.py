import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import uuid
import os

class CNCMachineDataGenerator:
    def __init__(self, kafka_bootstrap_servers='kafka:9092', topic='cnc_machine_sensor'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        
        # Initial normal operating parameters
        self.spindle_speed = 5000  # Normal range: 4800-5200 RPM
        self.cutting_force = 120  # Normal range: 110-130 N
        self.motor_temperature = 70  # Normal range: 65-75°C
        self.coolant_temperature = 25  # Normal range: 23-27°C
        self.coolant_flow_rate = 3.5  # Normal range: 3.3-3.7 L/min
        self.vibration = 0.4  # Normal range: 0.2-0.6 mm/s
        self.tool_wear = 0  # Percentage of wear (0-100%)
        self.operation_time = 0  # Hours of operation
        self.parts_produced = 0  # Number of parts produced
        
        # Maintenance interval for simulation purposes only
        self.maintenance_interval = 120  # Hours (e.g., 5 days)
    
    def generate_data(self):
        """Generate simulated CNC machine sensor readings"""
        machine_id = random.choice([
            "CNC-cd1809", "CNC-714ac9", "CNC-2645fa", "CNC-601eea"
        ])
        # Increment operation time and parts count
        time_increment = random.uniform(0.08, 0.25)  # 5-15 minutes in hour units
        self.operation_time += time_increment
        self.parts_produced += random.randint(1, 3)
        
        # Simulate wear over time
        wear_factor = self.operation_time / self.maintenance_interval
        
        # Fluctuate normal operating parameters
        if random.random() < 0.93:  # 93% of the time, normal operation with small fluctuations
            self.spindle_speed = 5000 + random.uniform(-200, 200) * (1 + 0.2 * wear_factor)
            self.cutting_force = 120 + random.uniform(-10, 10) * (1 + 0.3 * wear_factor)
            self.motor_temperature = 70 + random.uniform(-5, 5) * (1 + 0.5 * wear_factor)
            self.coolant_temperature = 25 + random.uniform(-2, 2) * (1 + 0.3 * wear_factor)
            self.coolant_flow_rate = 3.5 + random.uniform(-0.2, 0.2) * (1 - 0.3 * wear_factor)  # Flow decreases with wear
            self.vibration = 0.4 + random.uniform(-0.2, 0.2) * (1 + 0.6 * wear_factor)
            self.tool_wear = min(100, 50 * wear_factor + random.uniform(-5, 5))
        else:  # 7% of the time, simulate anomalies
            anomaly_type = random.choice(["spindle", "coolant", "temperature", "vibration"])
            if anomaly_type == "spindle":
                self.spindle_speed = random.choice([
                    random.uniform(4000, 4500),  # Low RPM
                    random.uniform(5500, 6000)   # High RPM
                ])
            elif anomaly_type == "coolant":
                self.coolant_flow_rate = random.uniform(1.8, 2.4)  # Low flow rate
                self.coolant_temperature = random.uniform(32, 38)  # High temperature
            elif anomaly_type == "temperature":
                self.motor_temperature = random.uniform(80, 95)  # High temperature
            elif anomaly_type == "vibration":
                self.vibration = random.uniform(1.0, 2.0)  # High vibration
        
        # Reset after maintenance
        if self.operation_time >= self.maintenance_interval:
            if random.random() < 0.8:  # 80% chance maintenance occurred
                self.operation_time = 0
                self.tool_wear = random.uniform(0, 5)
                self.coolant_flow_rate = 3.5 + random.uniform(-0.1, 0.1)
        
        # Create data payload
        timestamp = datetime.now().isoformat()
        data = {
            "timestamp": timestamp,
            "machine_id": machine_id,
            "machine_type": "CNC_MACHINE",
            "readings": {
                "spindle_speed_rpm": round(self.spindle_speed, 2),
                "cutting_force_n": round(self.cutting_force, 2),
                "motor_temperature_celsius": round(self.motor_temperature, 2),
                "coolant_temperature_celsius": round(self.coolant_temperature, 2),
                "coolant_flow_rate_l_min": round(self.coolant_flow_rate, 2),
                "vibration_mm_per_s": round(self.vibration, 3),
                "tool_wear_percent": round(self.tool_wear, 2),
                "operation_time_hours": round(self.operation_time, 2),
                "parts_produced": self.parts_produced
            }
        }
        
        return data

    def send_to_kafka(self, data):
        """Send data to Kafka topic"""
        self.producer.send(self.topic, data)
        
    def run(self, interval=5, iterations=None):
        """Run the data generator
        
        Args:
            interval (int): Interval between data points in seconds
            iterations (int, optional): Number of iterations to run. If None, runs indefinitely
        """
        count = 0
        try:
            while iterations is None or count < iterations:
                data = self.generate_data()
                self.send_to_kafka(data)
                print(f"Sent data to Kafka: {json.dumps(data, indent=2)}")
                time.sleep(interval)
                count += 1
        except KeyboardInterrupt:
            print("Data generation stopped")
        finally:
            self.producer.flush()
            self.producer.close()

if __name__ == "__main__":
    # Example usage
    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
    generator = CNCMachineDataGenerator(
        kafka_bootstrap_servers=KAFKA_BROKER,
        topic='cnc_machine_sensor'
    )
    generator.run(interval=5)  # Generate data every 5 seconds