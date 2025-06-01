import json
import random
import time
from datetime import datetime
import uuid
import pandas as pd
from tqdm import tqdm

class RoboticArmDatasetGenerator:
    def __init__(self, num_machines=4, time_periods=1000):
        self.num_machines = num_machines
        self.time_periods = time_periods
        self.machines = []
        self.dataset = []
        
        # Initialize machines with unique IDs
        for _ in range(num_machines):
            self.machines.append({
                "machine_id": f"ROB-ARM-{uuid.uuid4().hex[:6]}",
                "operation_time": 0,
                "cycle_count": 0,
                "joint_wear": 0,
                "maintenance_interval": random.randint(140, 200),  # Slight variation in maintenance needs
                "servo_motor_rpm": 1200,
                "hydraulic_pressure": 2000,
                "temperature": 65,
                "vibration": 0.5,
                "last_maintenance": 0,
                "failure_prone": random.random() < 0.3  # 30% of machines more prone to failures
            })
    
    def generate_machine_data(self, machine, time_step):
        """Generate data for a specific machine at a given time step"""
        # Increment operation time and cycle count
        time_increment = random.uniform(0.05, 0.2)  # 3-12 minutes in hour units
        machine["operation_time"] += time_increment
        machine["cycle_count"] += random.randint(5, 15)
        
        # Simulate wear over time
        wear_factor = machine["operation_time"] / machine["maintenance_interval"]
        
        # Condition flags
        servo_issue = False
        hydraulic_issue = False
        temp_issue = False
        vibration_issue = False
        
        # Failure probability increases with operation time and if machine is failure prone
        failure_prob = 0.05 + (0.15 * wear_factor)
        if machine["failure_prone"]:
            failure_prob *= 1.5
        
        # Fluctuate operating parameters based on machine condition
        if random.random() < 1 - failure_prob:  # Normal operation with small fluctuations
            machine["servo_motor_rpm"] = 1200 + random.uniform(-50, 50) * (1 + 0.3 * wear_factor)
            machine["hydraulic_pressure"] = 2000 + random.uniform(-100, 100) * (1 + 0.3 * wear_factor)
            machine["temperature"] = 65 + random.uniform(-5, 5) * (1 + 0.5 * wear_factor)
            machine["vibration"] = 0.5 + random.uniform(-0.2, 0.2) * (1 + 0.4 * wear_factor)
            machine["joint_wear"] = min(100, 45 * wear_factor + random.uniform(-5, 5))
        else:  # Simulate anomalies with higher probability as wear increases
            anomaly_type = random.choice(["servo", "hydraulic", "temperature", "vibration"])
            if anomaly_type == "servo":
                machine["servo_motor_rpm"] = random.choice([
                    random.uniform(900, 1100),  # Low RPM
                    random.uniform(1300, 1500)   # High RPM
                ])
                servo_issue = True
            elif anomaly_type == "hydraulic":
                machine["hydraulic_pressure"] = random.choice([
                    random.uniform(1600, 1800),  # Low pressure
                    random.uniform(2200, 2400)   # High pressure
                ])
                hydraulic_issue = True
            elif anomaly_type == "temperature":
                machine["temperature"] = random.uniform(75, 90)  # High temperature
                temp_issue = True
            elif anomaly_type == "vibration":
                machine["vibration"] = random.uniform(0.9, 2.0)  # High vibration
                vibration_issue = True
        
        # Determine if machine has failed
        severe_issues = sum([servo_issue, hydraulic_issue, temp_issue, vibration_issue])
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
            machine["joint_wear"] = random.uniform(0, 10)
            status = "MAINTENANCE_PERFORMED"
        
        # Create data entry
        timestamp = (datetime.now() + pd.Timedelta(hours=time_step)).isoformat()
        data = {
            "timestamp": timestamp,
            "machine_id": machine["machine_id"],
            "machine_type": "ROBOTIC_ARM",
            "servo_motor_rpm": round(machine["servo_motor_rpm"], 2),
            "hydraulic_pressure_psi": round(machine["hydraulic_pressure"], 2),
            "temperature_celsius": round(machine["temperature"], 2),
            "vibration_mm_per_s": round(machine["vibration"], 3),
            "operation_time_hours": round(machine["operation_time"], 2),
            "cycle_count": machine["cycle_count"],
            "joint_wear_percent": round(machine["joint_wear"], 2),
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
    
    def save_dataset(self, filename='robotic_arm_dataset.csv'):
        """Generate and save the dataset to a CSV file"""
        df = self.generate_dataset()
        df.to_csv(f"../dataset/{filename}", index=False)
        print(f"Dataset saved to ../dataset/{filename}")
        return df

if __name__ == "__main__":
    # Example usage
    generator = RoboticArmDatasetGenerator(num_machines=4, time_periods=3000)
    df = generator.save_dataset()
    
    # Print some statistics
    print("\nDataset Statistics:")
    print(f"Total records: {len(df)}")
    print(f"Unique machines: {df['machine_id'].nunique()}")
    print(f"Status distribution:\n{df['status'].value_counts()}")
    
    # Sample of the data
    print("\nSample data:")
    print(df.head())