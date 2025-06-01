import json
import random
import time
from datetime import datetime
import uuid
import pandas as pd
from tqdm import tqdm

class InjectionMoldingDatasetGenerator:
    def __init__(self, num_machines=4, time_periods=3000):
        self.num_machines = num_machines
        self.time_periods = time_periods
        self.machines = []
        self.dataset = []
        
        # Initialize machines with unique IDs
        for _ in range(num_machines):
            self.machines.append({
                "machine_id": f"INJ-MOLD-{uuid.uuid4().hex[:6]}",
                "operation_time": 0,
                "parts_produced": 0,
                "mold_wear": 0,
                "maintenance_interval": random.randint(140, 200),  # Variation in maintenance needs
                "last_maintenance": 0,
                # Normal operating parameters
                "barrel_temperature": 220,
                "mold_temperature": 45,
                "injection_pressure": 1200,
                "holding_pressure": 800,
                "injection_speed": 85,
                "cooling_time": 15,
                "cycle_time": 35,
                "material_viscosity": 320,
                "mold_clamping_force": 230,
                "failure_prone": random.random() < 0.25  # 25% of machines more prone to failures
            })
    
    def generate_machine_data(self, machine, time_step):
        """Generate data for a specific machine at a given time step"""
        # Increment operation time and parts count
        time_increment = random.uniform(0.05, 0.15)  # 3-9 minutes in hour units
        machine["operation_time"] += time_increment
        
        # Calculate parts produced based on cycle time
        cycles = (time_increment * 60 * 60) / machine["cycle_time"]
        machine["parts_produced"] += int(cycles)
        
        # Simulate wear over time
        wear_factor = machine["operation_time"] / machine["maintenance_interval"]
        
        # Condition flags
        temperature_issue = False
        pressure_issue = False
        viscosity_issue = False
        
        # Failure probability increases with operation time and if machine is failure prone
        failure_prob = 0.06 + (0.18 * wear_factor)
        if machine["failure_prone"]:
            failure_prob *= 1.5
        
        # Fluctuate normal operating parameters
        if random.random() < 1 - failure_prob:  # Normal operation with small fluctuations
            machine["barrel_temperature"] = 220 + random.uniform(-5, 5) * (1 + 0.2 * wear_factor)
            machine["mold_temperature"] = 45 + random.uniform(-5, 5) * (1 + 0.3 * wear_factor)
            machine["injection_pressure"] = 1200 + random.uniform(-50, 50) * (1 + 0.25 * wear_factor)
            machine["holding_pressure"] = 800 + random.uniform(-50, 50) * (1 + 0.2 * wear_factor)
            machine["injection_speed"] = 85 + random.uniform(-5, 5) * (1 - 0.1 * wear_factor)
            machine["cooling_time"] = 15 + random.uniform(-2, 2) * (1 + 0.15 * wear_factor)
            machine["cycle_time"] = 35 + random.uniform(-3, 3) * (1 + 0.2 * wear_factor)
            machine["material_viscosity"] = 320 + random.uniform(-20, 20) * (1 + 0.3 * wear_factor)
            machine["mold_clamping_force"] = 230 + random.uniform(-10, 10) * (1 - 0.1 * wear_factor)
            machine["mold_wear"] = min(100, 55 * wear_factor + random.uniform(-5, 5))
        else:  # Simulate anomalies
            anomaly_type = random.choice(["temperature", "pressure", "viscosity", "clogging"])
            if anomaly_type == "temperature":
                machine["barrel_temperature"] = random.choice([
                    random.uniform(195, 205),  # Low temperature
                    random.uniform(235, 245)   # High temperature
                ])
                machine["mold_temperature"] = random.uniform(55, 65)  # High mold temperature
                temperature_issue = True
            elif anomaly_type == "pressure":
                machine["injection_pressure"] = random.uniform(1300, 1400)  # High pressure
                machine["holding_pressure"] = random.uniform(900, 1000)  # High holding pressure
                pressure_issue = True
            elif anomaly_type == "viscosity":
                machine["material_viscosity"] = random.uniform(360, 400)  # High viscosity
                machine["injection_speed"] = random.uniform(70, 75)  # Lower speed due to high viscosity
                viscosity_issue = True
            elif anomaly_type == "clogging":
                machine["injection_pressure"] = random.uniform(1300, 1400)  # High pressure due to clogging
                machine["material_viscosity"] = random.uniform(360, 400)  # High viscosity due to clogging
                machine["cycle_time"] = random.uniform(42, 50)  # Longer cycle time
                pressure_issue = True
                viscosity_issue = True
        
        # Determine if machine has failed
        severe_issues = sum([temperature_issue, pressure_issue, viscosity_issue])
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
            machine["mold_wear"] = random.uniform(0, 10)
            status = "MAINTENANCE_PERFORMED"
        
        # Create data entry
        timestamp = (datetime.now() + pd.Timedelta(hours=time_step)).isoformat()
        data = {
            "timestamp": timestamp,
            "machine_id": machine["machine_id"],
            "machine_type": "INJECTION_MOLDING",
            "barrel_temperature_celsius": round(machine["barrel_temperature"], 2),
            "mold_temperature_celsius": round(machine["mold_temperature"], 2),
            "injection_pressure_bar": round(machine["injection_pressure"], 2),
            "holding_pressure_bar": round(machine["holding_pressure"], 2),
            "injection_speed_mm_per_s": round(machine["injection_speed"], 2),
            "cooling_time_s": round(machine["cooling_time"], 2),
            "cycle_time_s": round(machine["cycle_time"], 2),
            "material_viscosity_pa_s": round(machine["material_viscosity"], 2),
            "mold_clamping_force_tons": round(machine["mold_clamping_force"], 2),
            "operation_time_hours": round(machine["operation_time"], 2),
            "parts_produced": machine["parts_produced"],
            "mold_wear_percent": round(machine["mold_wear"], 2),
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
    
    def save_dataset(self, filename='../dataset/injection_molding_dataset.csv'):
        """Generate and save the dataset to a CSV file"""
        df = self.generate_dataset()
        df.to_csv(filename, index=False)
        print(f"Dataset saved to {filename}")
        return df

if __name__ == "__main__":
    # Example usage
    generator = InjectionMoldingDatasetGenerator(num_machines=6, time_periods=3000)
    df = generator.save_dataset()
    
    # Print some statistics
    print("\nDataset Statistics:")
    print(f"Total records: {len(df)}")
    print(f"Unique machines: {df['machine_id'].nunique()}")
    print(f"Status distribution:\n{df['status'].value_counts()}")
    
    # Sample of the data
    print("\nSample data:")
    print(df.head())