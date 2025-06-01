import json
import random
import time
from datetime import datetime
import uuid
import pandas as pd
from tqdm import tqdm

class CNCMachineDatasetGenerator:
    def __init__(self, num_machines=4, time_periods=3000):
        self.num_machines = num_machines
        self.time_periods = time_periods
        self.machines = []
        self.dataset = []
        
        # Initialize machines with unique IDs
        for _ in range(num_machines):
            self.machines.append({
                "machine_id": f"CNC-{uuid.uuid4().hex[:6]}",
                "operation_time": 0,
                "parts_produced": 0,
                "tool_wear": 0,
                "maintenance_interval": random.randint(100, 140),  # Slight variation in maintenance needs
                "spindle_speed": 5000,
                "cutting_force": 120,
                "motor_temperature": 70,
                "coolant_temperature": 25,
                "coolant_flow_rate": 3.5,
                "vibration": 0.4,
                "last_maintenance": 0,
                "failure_prone": random.random() < 0.25  # 25% of machines more prone to failures
            })
    
    def generate_machine_data(self, machine, time_step):
        """Generate data for a specific machine at a given time step"""
        # Increment operation time and parts count
        time_increment = random.uniform(0.08, 0.25)  # 5-15 minutes in hour units
        machine["operation_time"] += time_increment
        machine["parts_produced"] += random.randint(1, 3)
        
        # Simulate wear over time
        wear_factor = machine["operation_time"] / machine["maintenance_interval"]
        
        # Condition flags
        spindle_issue = False
        coolant_issue = False
        temp_issue = False
        vibration_issue = False
        
        # Failure probability increases with operation time and if machine is failure prone
        failure_prob = 0.05 + (0.15 * wear_factor)
        if machine["failure_prone"]:
            failure_prob *= 1.5
        
        # Fluctuate operating parameters based on machine condition
        if random.random() < 1 - failure_prob:  # Normal operation with small fluctuations
            machine["spindle_speed"] = 5000 + random.uniform(-200, 200) * (1 + 0.2 * wear_factor)
            machine["cutting_force"] = 120 + random.uniform(-10, 10) * (1 + 0.3 * wear_factor)
            machine["motor_temperature"] = 70 + random.uniform(-5, 5) * (1 + 0.5 * wear_factor)
            machine["coolant_temperature"] = 25 + random.uniform(-2, 2) * (1 + 0.3 * wear_factor)
            machine["coolant_flow_rate"] = 3.5 + random.uniform(-0.2, 0.2) * (1 - 0.3 * wear_factor)  # Flow decreases with wear
            machine["vibration"] = 0.4 + random.uniform(-0.2, 0.2) * (1 + 0.6 * wear_factor)
            machine["tool_wear"] = min(100, 50 * wear_factor + random.uniform(-5, 5))
        else:  # Simulate anomalies with higher probability as wear increases
            anomaly_type = random.choice(["spindle", "coolant", "temperature", "vibration"])
            if anomaly_type == "spindle":
                machine["spindle_speed"] = random.choice([
                    random.uniform(4000, 4500),  # Low RPM
                    random.uniform(5500, 6000)   # High RPM
                ])
                spindle_issue = True
            elif anomaly_type == "coolant":
                machine["coolant_flow_rate"] = random.uniform(1.8, 2.4)  # Low flow rate
                machine["coolant_temperature"] = random.uniform(32, 38)  # High temperature
                coolant_issue = True
            elif anomaly_type == "temperature":
                machine["motor_temperature"] = random.uniform(80, 95)  # High temperature
                temp_issue = True
            elif anomaly_type == "vibration":
                machine["vibration"] = random.uniform(1.0, 2.0)  # High vibration
                vibration_issue = True
        
        # Determine if machine has failed
        severe_issues = sum([spindle_issue, coolant_issue, temp_issue, vibration_issue])
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
            machine["tool_wear"] = random.uniform(0, 5)
            status = "MAINTENANCE_PERFORMED"
        
        # Create data entry
        timestamp = (datetime.now() + pd.Timedelta(hours=time_step)).isoformat()
        data = {
            "timestamp": timestamp,
            "machine_id": machine["machine_id"],
            "machine_type": "CNC_MACHINE",
            "spindle_speed_rpm": round(machine["spindle_speed"], 2),
            "cutting_force_n": round(machine["cutting_force"], 2),
            "motor_temperature_celsius": round(machine["motor_temperature"], 2),
            "coolant_temperature_celsius": round(machine["coolant_temperature"], 2),
            "coolant_flow_rate_l_min": round(machine["coolant_flow_rate"], 2),
            "vibration_mm_per_s": round(machine["vibration"], 3),
            "tool_wear_percent": round(machine["tool_wear"], 2),
            "operation_time_hours": round(machine["operation_time"], 2),
            "parts_produced": machine["parts_produced"],
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
    
    def save_dataset(self, filename='../dataset/cnc_machine_dataset.csv'):
        """Generate and save the dataset to a CSV file"""
        df = self.generate_dataset()
        df.to_csv(filename, index=False)
        print(f"Dataset saved to {filename}")
        return df

if __name__ == "__main__":
    # Example usage
    generator = CNCMachineDatasetGenerator(num_machines=4, time_periods=3000)
    df = generator.save_dataset()
    
    # Print some statistics
    print("\nDataset Statistics:")
    print(f"Total records: {len(df)}")
    print(f"Unique machines: {df['machine_id'].nunique()}")
    print(f"Status distribution:\n{df['status'].value_counts()}")
    
    # Sample of the data
    print("\nSample data:")
    print(df.head())