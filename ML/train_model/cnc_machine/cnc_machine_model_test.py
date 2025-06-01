import pandas as pd
import numpy as np
import json
from datetime import datetime
import joblib

class CNCFailurePredictor:
    def __init__(self, model_path='../../models/cnc_failure_prediction_model.pkl', feature_list_path='models/feature_columns.pkl'):
        """
        Initialize the CNC machine failure predictor
        
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
            'spindle_speed_rpm': 'spindle_speed_rpm',
            'cutting_force_n': 'cutting_force_n',
            'motor_temperature_celsius': 'motor_temperature_celsius',
            'coolant_temperature_celsius': 'coolant_temperature_celsius',
            'coolant_flow_rate_l_min': 'coolant_flow_rate_l_min',
            'vibration_mm_per_s': 'vibration_mm_per_s',
            'tool_wear_percent': 'tool_wear_percent',
            'operation_time_hours': 'operation_time_hours',
            'parts_produced': 'parts_produced',
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
        # Speed-temperature ratio
        features_df['speed_temp_ratio'] = features_df['spindle_speed_rpm'] / np.maximum(features_df['motor_temperature_celsius'], 0.1)
        # Wear per hour
        features_df['wear_per_hour'] = features_df['tool_wear_percent'] / np.maximum(features_df['operation_time_hours'], 0.1)
        # Coolant efficiency
        features_df['coolant_efficiency'] = features_df['coolant_flow_rate_l_min'] / np.maximum(features_df['coolant_temperature_celsius'], 0.1)
        # Vibration per speed
        features_df['vibration_per_speed'] = features_df['vibration_mm_per_s'] * 1000 / np.maximum(features_df['spindle_speed_rpm'], 0.1)
        
        # Handle missing features (lagged and change features)
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

    predictor = CNCFailurePredictor()
    
    sensor_data = {
        "timestamp": datetime.now(),
        "machine_id": " CNC-cd1809",
        "machine_type": "CNC_MACHINE",
        "readings": {
            "spindle_speed_rpm": 4999.74,
            "cutting_force_n": 110.19,
            "motor_temperature_celsius":  69.36,
            "coolant_temperature_celsius": 26.31,
            "coolant_flow_rate_l_min": 3.52,
            "vibration_mm_per_s": 0.503,
            "tool_wear_percent": 0.94,
            "operation_time_hours": 0.15,
            "parts_produced": 2
        }
    }

    result = predictor.predict_failure_probability(sensor_data)
    print(result)