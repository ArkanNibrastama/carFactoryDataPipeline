import json
import random
import time
from datetime import datetime
import uuid
import pandas as pd
from tqdm import tqdm

class BatteryAssemblyDatasetGenerator:
    def __init__(self, num_machines=4, time_periods=3000):
        self.num_machines = num_machines
        self.time_periods = time_periods
        self.machines = []
        self.dataset = []
        
        # Initialize machines with unique IDs
        for _ in range(num_machines):
            self.machines.append({
                "machine_id": f"BAT-ASM-{uuid.uuid4().hex[:6]}",
                "operation_time": 0,
                "batteries_produced": 0,
                "electrode_wear": 0,
                "maintenance_interval": random.randint(90, 110),  # Slight variation in maintenance needs
                "welding_current": 150,
                "welding_voltage": 28,
                "electrode_temperature": 320,
                "cooling_system_temperature": 25,
                "alignment_accuracy": 0.05,
                "vacuum_pressure": 55,
                "cell_voltage_uniformity": 0.98,
                "insulation_resistance": 500,
                "leakage_current": 0.02,
                "last_maintenance": 0,
                "failure_prone": random.random() < 0.25  # 25% of machines more prone to failures
            })
    
    def generate_machine_data(self, machine, time_step):
        """Generate data for a specific machine at a given time step"""
        # Increment operation time and battery count
        time_increment = random.uniform(0.04, 0.12)  # 2.4-7.2 minutes in hour units
        machine["operation_time"] += time_increment
        
        # Calculate batteries produced
        production_rate = random.uniform(12, 20)  # Batteries per hour
        machine["batteries_produced"] += int(time_increment * production_rate)
        
        # Simulate wear over time
        wear_factor = machine["operation_time"] / machine["maintenance_interval"]
        
        # Condition flags
        welding_issue = False
        temp_issue = False
        alignment_issue = False
        electrical_issue = False
        
        # Failure probability increases with operation time and if machine is failure prone
        failure_prob = 0.05 + (0.15 * wear_factor)
        if machine["failure_prone"]:
            failure_prob *= 1.5
        
        # Fluctuate operating parameters based on machine condition
        if random.random() < 1 - failure_prob:  # Normal operation with small fluctuations
            machine["welding_current"] = 150 + random.uniform(-5, 5) * (1 + 0.2 * wear_factor)
            machine["welding_voltage"] = 28 + random.uniform(-1, 1) * (1 + 0.15 * wear_factor)
            machine["electrode_temperature"] = 320 + random.uniform(-10, 10) * (1 + 0.3 * wear_factor)
            machine["cooling_system_temperature"] = 25 + random.uniform(-3, 3) * (1 + 0.4 * wear_factor)
            machine["alignment_accuracy"] = 0.05 + random.uniform(-0.02, 0.02) * (1 + 0.5 * wear_factor)
            machine["vacuum_pressure"] = 55 + random.uniform(-2, 2) * (1 - 0.2 * wear_factor)
            machine["cell_voltage_uniformity"] = 0.98 - random.uniform(0, 0.01) * wear_factor
            machine["insulation_resistance"] = 500 - random.uniform(0, 20) * wear_factor
            machine["leakage_current"] = 0.02 + random.uniform(0, 0.01) * wear_factor
            machine["electrode_wear"] = min(100, 60 * wear_factor + random.uniform(-5, 5))
        else:  # Simulate anomalies
            anomaly_type = random.choice(["welding", "temperature", "alignment", "electrical"])
            if anomaly_type == "welding":
                machine["welding_current"] = random.choice([
                    random.uniform(130, 140),  # Low current
                    random.uniform(160, 170)   # High current
                ])
                machine["welding_voltage"] = random.choice([
                    random.uniform(25, 26),    # Low voltage
                    random.uniform(30, 31)     # High voltage
                ])
                machine["electrode_temperature"] = random.uniform(340, 360)  # High temperature
                welding_issue = True
            elif anomaly_type == "temperature":
                machine["electrode_temperature"] = random.uniform(345, 365)  # High electrode temperature
                machine["cooling_system_temperature"] = random.uniform(33, 38)  # High cooling system temperature
                temp_issue = True
            elif anomaly_type == "alignment":
                machine["alignment_accuracy"] = random.uniform(0.09, 0.15)  # Poor alignment
                machine["vacuum_pressure"] = random.uniform(45, 50)  # Low vacuum pressure
                alignment_issue = True
            elif anomaly_type == "electrical":
                machine["cell_voltage_uniformity"] = random.uniform(0.90, 0.94)  # Poor cell uniformity
                machine["insulation_resistance"] = random.uniform(350, 400)  # Low insulation resistance
                machine["leakage_current"] = random.uniform(0.07, 0.12)  # High leakage current
                electrical_issue = True
        
        # Determine if machine has failed
        severe_issues = sum([welding_issue, temp_issue, alignment_issue, electrical_issue])
        machine_failed = False
        
        if severe_issues >= 2:
            machine_failed = True  # Multiple issues cause failure
        elif severe_issues == 1 and random.random() < wear_factor:
            machine_failed = True  # Single issue more likely to cause failure as wear increases
        elif wear_factor > 0.9 and random.random() < 0.4:
            machine_failed = True  # Wear-related failure
        
        # Calculate time since last maintenance
        time_since_maintenance = machine["operation_time"] - machine["last_maintenance"]
        
        # Determine status
        if machine_failed:
            status = "FAILURE"
        elif time_since_maintenance >= machine["maintenance_interval"]:
            status = "MAINTENANCE_NEEDED"
        else:
            status = "NORMAL"
        
        # Reset after maintenance (when status is maintenance needed, 80% chance maintenance occurred)
        if status == "MAINTENANCE_NEEDED" and random.random() < 0.8:
            machine["last_maintenance"] = machine["operation_time"]
            machine["electrode_wear"] = random.uniform(0, 5)
            status = "MAINTENANCE_PERFORMED"
        
        # Create data entry
        timestamp = (datetime.now() + pd.Timedelta(hours=time_step)).isoformat()
        data = {
            "timestamp": timestamp,
            "machine_id": machine["machine_id"],
            "machine_type": "BATTERY_ASSEMBLY",
            "welding_current_amps": round(machine["welding_current"], 2),
            "welding_voltage_volts": round(machine["welding_voltage"], 2),
            "electrode_temperature_celsius": round(machine["electrode_temperature"], 2),
            "cooling_system_temperature_celsius": round(machine["cooling_system_temperature"], 2),
            "alignment_accuracy_mm": round(machine["alignment_accuracy"], 3),
            "vacuum_pressure_kpa": round(machine["vacuum_pressure"], 2),
            "cell_voltage_uniformity_ratio": round(machine["cell_voltage_uniformity"], 4),
            "insulation_resistance_mohm": round(machine["insulation_resistance"], 2),
            "leakage_current_ma": round(machine["leakage_current"], 3),
            "electrode_wear_percent": round(machine["electrode_wear"], 2),
            "operation_time_hours": round(machine["operation_time"], 2),
            "batteries_produced": machine["batteries_produced"],
            "status": status,
            "time_since_maintenance": round(time_since_maintenance, 2)
        }
        
        return data
    
    def generate_dataset(self):
        """Generate the full dataset for all machines and time periods"""
        print("Generating dataset...")
        for time_step in tqdm(range(self.time_periods)):
            for machine in self.machines:
                data = self.generate_machine_data(machine, time_step)
                self.dataset.append(data)
        
        # Convert to DataFrame
        df = pd.DataFrame(self.dataset)
        
        # Add some derived features that might be useful for prediction
        df['hour_of_day'] = pd.to_datetime(df['timestamp']).dt.hour
        df['day_of_week'] = pd.to_datetime(df['timestamp']).dt.dayofweek
        
        return df
    
    def save_dataset(self, filename='../dataset/battery_assembly_dataset.csv'):
        """Generate and save the dataset to a CSV file"""
        
        df = self.generate_dataset()
        df.to_csv(filename, index=False)
        print(f"Dataset saved to {filename}")
        return df

if __name__ == "__main__":
    # Example usage
    generator = BatteryAssemblyDatasetGenerator(num_machines=4, time_periods=3000)
    df = generator.save_dataset()
    
    # Print some statistics
    print("\nDataset Statistics:")
    print(f"Total records: {len(df)}")
    print(f"Unique machines: {df['machine_id'].nunique()}")
    print(f"Status distribution:\n{df['status'].value_counts()}")
    
    # Sample of the data
    print("\nSample data:")
    print(df.head())