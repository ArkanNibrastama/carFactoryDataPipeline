import pandas as pd
import numpy as np
import json
from datetime import datetime
import joblib

class BatteryAssemblyFailurePredictor:
    def __init__(self, model_path='../../models/battery_failure_prediction_model.pkl', feature_list_path='models/battery_feature_columns.pkl'):
        """
        Initialize the battery assembly machine failure predictor
        
        Args:
            model_path (str): Path to the trained model
            feature_list_path (str): Path to the feature list
        """
        self.model = joblib.load(model_path)
        self.feature_columns = joblib.load(feature_list_path)
        print(f"Loaded model and {len(self.feature_columns)} features")
    
    def predict_failure_probability(self, machine_data):
        """
        Predict the probability of failure for a machine
        
        Args:
            machine_data (dict): Machine sensor readings
            
        Returns:
            dict: Prediction results including failure probability and risk level
        """
        # Extract readings
        if 'readings' in machine_data:
            readings = machine_data.get('readings', {})
        else:
            readings = machine_data  # Assume direct readings if 'readings' key not present
        
        # Prepare data point for prediction
        data_point = {}
        
        # Map the input fields to the expected feature names
        sensor_feature_map = {
            'welding_current_amps': 'welding_current_amps',
            'welding_voltage_volts': 'welding_voltage_volts',
            'electrode_temperature_celsius': 'electrode_temperature_celsius',
            'cooling_system_temperature_celsius': 'cooling_system_temperature_celsius',
            'alignment_accuracy_mm': 'alignment_accuracy_mm',
            'vacuum_pressure_kpa': 'vacuum_pressure_kpa',
            'cell_voltage_uniformity_ratio': 'cell_voltage_uniformity_ratio',
            'insulation_resistance_mohm': 'insulation_resistance_mohm',
            'leakage_current_ma': 'leakage_current_ma',
            'electrode_wear_percent': 'electrode_wear_percent',
            'operation_time_hours': 'operation_time_hours',
            'batteries_produced': 'batteries_produced',
            'time_since_maintenance': 'time_since_maintenance'
        }
        
        # Fill in available sensor data
        for feature_name, reading_name in sensor_feature_map.items():
            data_point[feature_name] = readings.get(reading_name, 0)
        
        # Add time-based features
        current_time = datetime.now()
        data_point['hour_of_day'] = current_time.hour
        data_point['day_of_week'] = current_time.weekday()
        
        # Create DataFrame with single row
        features_df = pd.DataFrame([data_point])
        
        # Add engineered features similar to training
        # Current-voltage ratio
        features_df['current_voltage_ratio'] = features_df['welding_current_amps'] / np.maximum(features_df['welding_voltage_volts'], 0.1)
        # Electrode wear per hour
        features_df['electrode_wear_per_hour'] = features_df['electrode_wear_percent'] / np.maximum(features_df['operation_time_hours'], 0.1)
        # Temperature difference
        features_df['temp_diff'] = features_df['electrode_temperature_celsius'] - features_df['cooling_system_temperature_celsius']
        # Electrical stability
        features_df['electrical_stability'] = features_df['cell_voltage_uniformity_ratio'] * features_df['insulation_resistance_mohm'] / np.maximum(features_df['leakage_current_ma'], 0.001)
        
        for col in self.feature_columns:
            if col not in features_df.columns:
                if '_prev' in col:
                    base_col = col.replace('_prev', '')
                    if base_col in features_df.columns:
                        features_df[col] = features_df[base_col]
                    else:
                        features_df[col] = 0
                elif '_change' in col:
                    features_df[col] = 0
                else:
                    features_df[col] = 0
        
        # Select features in the same order as during training
        prediction_features = features_df[self.feature_columns]
        
        # Predict probability
        failure_prob = self.model.predict_proba(prediction_features)[0][1]
        failure_prediction = self.model.predict(prediction_features)[0]
        
        # Determine risk level
        if failure_prob < 0.3:
            risk_level = "LOW"
        elif failure_prob < 0.7:
            risk_level = "MEDIUM"
        else:
            risk_level = "HIGH"
        
        # Return prediction results
        return {
            "machine_id": machine_data.get('machine_id', 'unknown'),
            "failure_probability": round(failure_prob, 3),
            "failure_predicted": bool(failure_prediction),
            "risk_level": risk_level,
            "prediction_timestamp": datetime.now().isoformat()
        }
    
if __name__ == "__main__":

    predictor = BatteryAssemblyFailurePredictor()

    sensor_data = {
        "timestamp": datetime.now(),
        "machine_id": "BAT-ASM-8a8c5b",
        "machine_type": "BATTERY_ASSEMBLY",
        "readings": {
            "welding_current_amps": 150.0,
            "welding_voltage_volts": 28.0,
            "electrode_temperature_celsius": 361.71,
            "cooling_system_temperature_celsius": 36.83,
            "alignment_accuracy_mm": 0.05,
            "vacuum_pressure_kpa": 55.0,
            "cell_voltage_uniformity_ratio": 0.98,
            "insulation_resistance_mohm": 500.0,
            "leakage_current_ma": 0.02,
            "operation_time_hours": 0.7,
            "batteries_produced": 7,
            "electrode_wear_percent": 70
        }
    }

    result = predictor.predict_failure_probability(sensor_data)
    print(result)