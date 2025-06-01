import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import uuid
import os

class RoboticArmDataGenerator:
    def __init__(self, kafka_bootstrap_servers='kafka:9092', topic='arm_robot_machine_sensor'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        
        # Initial normal operating parameters
        self.servo_motor_rpm = 1200  # Normal RPM range: 1150-1250
        self.hydraulic_pressure = 2000  # Normal range: 1900-2100 PSI
        self.temperature = 65  # Normal range: 60-70°C
        self.vibration = 0.5  # Normal range: 0.3-0.7 mm/s
        self.operation_time = 0  # Hours since last maintenance
        self.cycle_count = 0  # Number of welding/assembly cycles completed
        self.joint_wear = 0  # Percentage of wear (0-100%)
        
        # Maintenance intervals for simulation purposes only
        self.maintenance_interval = 168  # Hours (e.g., 7 days)
    
    def generate_data(self):
        """Generate simulated robotic arm sensor readings"""
        machine_id = random.choice(["ROB-ARM-11cb98", "ROB-ARM-5756d3", "ROB-ARM-cb4fee", "ROB-ARM-241e51"])

        # Increment operation time and cycle count
        time_increment = random.uniform(0.05, 0.2)  # 3-12 minutes in hour units
        self.operation_time += time_increment
        self.cycle_count += random.randint(5, 15)
        
        # Simulate wear over time
        wear_factor = self.operation_time / self.maintenance_interval
        
        # Fluctuate normal operating parameters
        if random.random() < 0.95:  # 95% of the time, normal operation with small fluctuations
            self.servo_motor_rpm = 1200 + random.uniform(-50, 50) * (1 + 0.3 * wear_factor)
            self.hydraulic_pressure = 2000 + random.uniform(-100, 100) * (1 + 0.3 * wear_factor)
            self.temperature = 65 + random.uniform(-5, 5) * (1 + 0.5 * wear_factor)
            self.vibration = 0.5 + random.uniform(-0.2, 0.2) * (1 + 0.4 * wear_factor)
            self.joint_wear = min(100, 45 * wear_factor + random.uniform(-5, 5))
        else:  # 5% of the time, simulate anomalies
            anomaly_type = random.choice(["servo", "hydraulic", "temperature", "vibration"])
            if anomaly_type == "servo":
                self.servo_motor_rpm = random.choice([
                    random.uniform(900, 1100),  # Low RPM
                    random.uniform(1300, 1500)   # High RPM
                ])
            elif anomaly_type == "hydraulic":
                self.hydraulic_pressure = random.choice([
                    random.uniform(1600, 1800),  # Low pressure
                    random.uniform(2200, 2400)   # High pressure
                ])
            elif anomaly_type == "temperature":
                self.temperature = random.uniform(75, 90)  # High temperature
            elif anomaly_type == "vibration":
                self.vibration = random.uniform(0.9, 2.0)  # High vibration
        
        # Reset after maintenance
        if self.operation_time >= self.maintenance_interval:
            if random.random() < 0.7:  # 70% chance maintenance occurred
                self.operation_time = 0
                self.joint_wear = random.uniform(0, 10)
        
        # Create data payload
        timestamp = datetime.now().isoformat()
        data = {
            "timestamp": timestamp,
            "machine_id": machine_id,
            "machine_type": "ROBOTIC_ARM",
            "readings": {
                "servo_motor_rpm": round(self.servo_motor_rpm, 2),
                "hydraulic_pressure_psi": round(self.hydraulic_pressure, 2),
                "temperature_celsius": round(self.temperature, 2),
                "vibration_mm_per_s": round(self.vibration, 3),
                "operation_time_hours": round(self.operation_time, 2),
                "cycle_count": self.cycle_count,
                "joint_wear_percent": round(self.joint_wear, 2)
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

    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
    # Example usage
    generator = RoboticArmDataGenerator(
        kafka_bootstrap_servers=KAFKA_BROKER,
        topic='arm_robot_machine_sensor'
    )
    generator.run(interval=5)  # Generate data every 5 seconds