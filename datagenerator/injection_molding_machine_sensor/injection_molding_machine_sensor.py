import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import uuid
import os

class InjectionMoldingDataGenerator:
    def __init__(self, kafka_bootstrap_servers='kafka:9092', topic='injection_molding_machine_sensor'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        
        # Initial normal operating parameters
        self.barrel_temperature = 220  # Normal range: 215-225°C
        self.mold_temperature = 45  # Normal range: 40-50°C
        self.injection_pressure = 1200  # Normal range: 1150-1250 bar
        self.holding_pressure = 800  # Normal range: 750-850 bar
        self.injection_speed = 85  # Normal range: 80-90 mm/s
        self.cooling_time = 15  # Normal range: 13-17 seconds
        self.cycle_time = 35  # Normal range: 32-38 seconds
        self.material_viscosity = 320  # Normal range: 300-340 Pa·s
        self.mold_clamping_force = 230  # Normal range: 220-240 tons
        self.operation_time = 0  # Hours of operation
        self.parts_produced = 0  # Number of parts produced
        self.mold_wear = 0  # Percentage of wear (0-100%)
        
        # Maintenance interval for simulation purposes only
        self.maintenance_interval = 144  # Hours (e.g., 6 days)
    
    def generate_data(self):
        """Generate simulated injection molding machine sensor readings"""
        machine_id = random.choice([
            "INJ-MOLD-78f8c6", "INJ-MOLD-0b44f3", "INJ-MOLD-8dc0cd", "INJ-MOLD-d8eee5",
            "INJ-MOLD-aaa028", "INJ-MOLD-6caf79"
        ])
        # Increment operation time and parts count
        time_increment = random.uniform(0.05, 0.15)  # 3-9 minutes in hour units
        self.operation_time += time_increment
        
        # Calculate parts produced based on cycle time
        cycles = (time_increment * 60 * 60) / self.cycle_time
        self.parts_produced += int(cycles)
        
        # Simulate wear over time
        wear_factor = self.operation_time / self.maintenance_interval
        
        # Fluctuate normal operating parameters
        if random.random() < 0.94:  # 94% of the time, normal operation with small fluctuations
            self.barrel_temperature = 220 + random.uniform(-5, 5) * (1 + 0.2 * wear_factor)
            self.mold_temperature = 45 + random.uniform(-5, 5) * (1 + 0.3 * wear_factor)
            self.injection_pressure = 1200 + random.uniform(-50, 50) * (1 + 0.25 * wear_factor)
            self.holding_pressure = 800 + random.uniform(-50, 50) * (1 + 0.2 * wear_factor)
            self.injection_speed = 85 + random.uniform(-5, 5) * (1 - 0.1 * wear_factor)
            self.cooling_time = 15 + random.uniform(-2, 2) * (1 + 0.15 * wear_factor)
            self.cycle_time = 35 + random.uniform(-3, 3) * (1 + 0.2 * wear_factor)
            self.material_viscosity = 320 + random.uniform(-20, 20) * (1 + 0.3 * wear_factor)
            self.mold_clamping_force = 230 + random.uniform(-10, 10) * (1 - 0.1 * wear_factor)
            self.mold_wear = min(100, 55 * wear_factor + random.uniform(-5, 5))
        else:  # 6% of the time, simulate anomalies
            anomaly_type = random.choice(["temperature", "pressure", "viscosity", "clogging"])
            if anomaly_type == "temperature":
                self.barrel_temperature = random.choice([
                    random.uniform(195, 205),  # Low temperature
                    random.uniform(235, 245)   # High temperature
                ])
                self.mold_temperature = random.uniform(55, 65)  # High mold temperature
            elif anomaly_type == "pressure":
                self.injection_pressure = random.uniform(1300, 1400)  # High pressure
                self.holding_pressure = random.uniform(900, 1000)  # High holding pressure
            elif anomaly_type == "viscosity":
                self.material_viscosity = random.uniform(360, 400)  # High viscosity
                self.injection_speed = random.uniform(70, 75)  # Lower speed due to high viscosity
            elif anomaly_type == "clogging":
                self.injection_pressure = random.uniform(1300, 1400)  # High pressure due to clogging
                self.material_viscosity = random.uniform(360, 400)  # High viscosity
                self.cycle_time = random.uniform(42, 50)  # Longer cycle time
        
        # Reset after maintenance
        if self.operation_time >= self.maintenance_interval:
            if random.random() < 0.75:  # 75% chance maintenance occurred
                self.operation_time = 0
                self.mold_wear = random.uniform(0, 10)
        
        # Create data payload
        timestamp = datetime.now().isoformat()
        data = {
            "timestamp": timestamp,
            "machine_id": machine_id,
            "machine_type": "INJECTION_MOLDING",
            "readings": {
                "barrel_temperature_celsius": round(self.barrel_temperature, 2),
                "mold_temperature_celsius": round(self.mold_temperature, 2),
                "injection_pressure_bar": round(self.injection_pressure, 2),
                "holding_pressure_bar": round(self.holding_pressure, 2),
                "injection_speed_mm_per_s": round(self.injection_speed, 2),
                "cooling_time_s": round(self.cooling_time, 2),
                "cycle_time_s": round(self.cycle_time, 2),
                "material_viscosity_pa_s": round(self.material_viscosity, 2),
                "mold_clamping_force_tons": round(self.mold_clamping_force, 2),
                "operation_time_hours": round(self.operation_time, 2),
                "parts_produced": self.parts_produced,
                "mold_wear_percent": round(self.mold_wear, 2)
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
    generator = InjectionMoldingDataGenerator(
        kafka_bootstrap_servers=KAFKA_BROKER,
        topic='injection_molding_machine_sensor'
    )
    generator.run(interval=5)  # Generate data every 5 seconds