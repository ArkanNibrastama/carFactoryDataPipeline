import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
import uuid
import os

class BatteryAssemblyDataGenerator:
    def __init__(self, kafka_bootstrap_servers='kafka:9092', topic='battery_assembly_machine_sensor'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.topic = topic
        
        # Initial normal operating parameters
        self.welding_current = 150  # Normal range: 145-155 Amps
        self.welding_voltage = 28  # Normal range: 27-29 Volts
        self.electrode_temperature = 320  # Normal range: 310-330°C
        self.cooling_system_temperature = 25  # Normal range: 22-28°C
        self.alignment_accuracy = 0.05  # Normal range: 0.03-0.07 mm
        self.vacuum_pressure = 55  # Normal range: 53-57 kPa
        self.cell_voltage_uniformity = 0.98  # Normal range: 0.97-0.99 (ratio)
        self.insulation_resistance = 500  # Normal range: 480-520 MOhm
        self.leakage_current = 0.02  # Normal range: 0.01-0.03 mA
        self.operation_time = 0  # Hours of operation
        self.batteries_produced = 0  # Number of batteries produced
        self.electrode_wear = 0  # Percentage of wear (0-100%)
        
        # Maintenance interval for simulation purposes only
        self.maintenance_interval = 96  # Hours (e.g., 4 days)
    
    def generate_data(self):
        """Generate simulated battery assembly line sensor readings"""
        machine_id = random.choice([
            "BAT-ASM-8a8c5b", "BAT-ASM-cb1136", "BAT-ASM-6dbfd2", "BAT-ASM-44f453"
        ])
        # Increment operation time and battery count
        time_increment = random.uniform(0.04, 0.12)  # 2.4-7.2 minutes in hour units
        self.operation_time += time_increment
        
        # Calculate batteries produced (assuming one battery every 3-5 minutes)
        production_rate = random.uniform(12, 20)  # Batteries per hour
        self.batteries_produced += int(time_increment * production_rate)
        
        # Simulate wear over time
        wear_factor = self.operation_time / self.maintenance_interval
        
        # Fluctuate normal operating parameters
        if random.random() < 0.92:  # 92% of the time, normal operation with small fluctuations
            self.welding_current = 150 + random.uniform(-5, 5) * (1 + 0.2 * wear_factor)
            self.welding_voltage = 28 + random.uniform(-1, 1) * (1 + 0.15 * wear_factor)
            self.electrode_temperature = 320 + random.uniform(-10, 10) * (1 + 0.3 * wear_factor)
            self.cooling_system_temperature = 25 + random.uniform(-3, 3) * (1 + 0.4 * wear_factor)
            self.alignment_accuracy = 0.05 + random.uniform(-0.02, 0.02) * (1 + 0.5 * wear_factor)
            self.vacuum_pressure = 55 + random.uniform(-2, 2) * (1 - 0.2 * wear_factor)
            self.cell_voltage_uniformity = 0.98 - random.uniform(0, 0.01) * wear_factor
            self.insulation_resistance = 500 - random.uniform(0, 20) * wear_factor
            self.leakage_current = 0.02 + random.uniform(0, 0.01) * wear_factor
            self.electrode_wear = min(100, 60 * wear_factor + random.uniform(-5, 5))
        else:  # 8% of the time, simulate anomalies
            anomaly_type = random.choice(["welding", "temperature", "alignment", "electrical"])
            if anomaly_type == "welding":
                self.welding_current = random.choice([
                    random.uniform(130, 140),  # Low current
                    random.uniform(160, 170)   # High current
                ])
                self.welding_voltage = random.choice([
                    random.uniform(25, 26),    # Low voltage
                    random.uniform(30, 31)     # High voltage
                ])
                self.electrode_temperature = random.uniform(340, 360)  # High temperature
            elif anomaly_type == "temperature":
                self.electrode_temperature = random.uniform(345, 365)  # High electrode temperature
                self.cooling_system_temperature = random.uniform(33, 38)  # High cooling system temperature
            elif anomaly_type == "alignment":
                self.alignment_accuracy = random.uniform(0.09, 0.15)  # Poor alignment
                self.vacuum_pressure = random.uniform(45, 50)  # Low vacuum pressure
            elif anomaly_type == "electrical":
                self.cell_voltage_uniformity = random.uniform(0.90, 0.94)  # Poor cell uniformity
                self.insulation_resistance = random.uniform(350, 400)  # Low insulation resistance
                self.leakage_current = random.uniform(0.07, 0.12)  # High leakage current
        
        # Reset after maintenance
        if self.operation_time >= self.maintenance_interval:
            if random.random() < 0.85:  # 85% chance maintenance occurred
                self.operation_time = 0
                self.electrode_wear = random.uniform(0, 5)
                self.cooling_system_temperature = 25 + random.uniform(-2, 2)
        
        # Create data payload
        timestamp = datetime.now().isoformat()
        data = {
            "timestamp": timestamp,
            "machine_id": machine_id,
            "machine_type": "BATTERY_ASSEMBLY",
            "readings": {
                "welding_current_amps": round(self.welding_current, 2),
                "welding_voltage_volts": round(self.welding_voltage, 2),
                "electrode_temperature_celsius": round(self.electrode_temperature, 2),
                "cooling_system_temperature_celsius": round(self.cooling_system_temperature, 2),
                "alignment_accuracy_mm": round(self.alignment_accuracy, 3),
                "vacuum_pressure_kpa": round(self.vacuum_pressure, 2),
                "cell_voltage_uniformity_ratio": round(self.cell_voltage_uniformity, 4),
                "insulation_resistance_mohm": round(self.insulation_resistance, 2),
                "leakage_current_ma": round(self.leakage_current, 3),
                "operation_time_hours": round(self.operation_time, 2),
                "batteries_produced": self.batteries_produced,
                "electrode_wear_percent": round(self.electrode_wear, 2)
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

    generator = BatteryAssemblyDataGenerator(
        kafka_bootstrap_servers=KAFKA_BROKER,
        topic='battery_assembly_machine_sensor'
    )
    generator.run(interval=5)  # Generate data every 5 seconds