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
        self.producer.send(self.topic, data)
        
    def run(self):
        data = self.generate_data()
        self.send_to_kafka(data)
        print(f"Sent data to Kafka: {json.dumps(data, indent=2)}")

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
        self.producer.send(self.topic, data)
        
    def run(self):
        data = self.generate_data()
        self.send_to_kafka(data)
        print(f"Sent data to Kafka: {json.dumps(data, indent=2)}")

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
        self.producer.send(self.topic, data)
        
    def run(self):
        data = self.generate_data()
        self.send_to_kafka(data)
        print(f"Sent data to Kafka: {json.dumps(data, indent=2)}")
                
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
        self.producer.send(self.topic, data)
        
    def run(self):
        data = self.generate_data()
        self.send_to_kafka(data)
        print(f"Sent data to Kafka: {json.dumps(data, indent=2)}")
                


if __name__ == "__main__":

    KAFKA_BROKER = os.getenv("KAFKA_BROKER")
    KAFKA_TOPIC_ARM_ROBOT = os.getenv("KAFKA_TOPIC_ARM_ROBOT")
    KAFKA_TOPIC_CNC = os.getenv("KAFKA_TOPIC_CNC")
    KAFKA_TOPIC_INJECTION_MOLDING = os.getenv("KAFKA_TOPIC_INJECTION_MOLDING")
    KAFKA_TOPIC_BATTERY_ASSEMBLY = os.getenv("KAFKA_TOPIC_BATTERY_ASSEMBLY")
   
    sensor_arm_robot = RoboticArmDataGenerator(
        kafka_bootstrap_servers=KAFKA_BROKER  ,
        topic=KAFKA_TOPIC_ARM_ROBOT
    )

    sensor_cnc_robot = CNCMachineDataGenerator(
        kafka_bootstrap_servers=KAFKA_BROKER,
        topic=KAFKA_TOPIC_CNC
    )

    sensor_injection_molding_robot = InjectionMoldingDataGenerator(
        kafka_bootstrap_servers=KAFKA_BROKER,
        topic=KAFKA_TOPIC_INJECTION_MOLDING
    )

    sensor_battery_assembly_robot = BatteryAssemblyDataGenerator(
        kafka_bootstrap_servers=KAFKA_BROKER,
        topic=KAFKA_TOPIC_BATTERY_ASSEMBLY
    )
    
    # here is looping
    try:
        while True:
            sensor_arm_robot.run()
            time.sleep(0.1)
            sensor_cnc_robot.run()
            time.sleep(0.1)
            sensor_injection_molding_robot.run()
            time.sleep(0.1)
            sensor_battery_assembly_robot.run()
            time.sleep(5)
    except KeyboardInterrupt:
        print("Data generation stopped")
    finally:
        # flush the left data
        sensor_arm_robot.producer.flush()
        sensor_cnc_robot.producer.flush()
        sensor_injection_molding_robot.producer.flush()
        sensor_battery_assembly_robot.producer.flush()

        # then close it
        sensor_arm_robot.producer.close()
        sensor_cnc_robot.producer.close()
        sensor_injection_molding_robot.producer.close()
        sensor_battery_assembly_robot.producer.close()