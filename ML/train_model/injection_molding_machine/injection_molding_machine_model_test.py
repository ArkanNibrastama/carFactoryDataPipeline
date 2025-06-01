from datetime import datetime
import pandas as pd
import numpy as np
import joblib
import json

class InjectionMoldingPredictor:
    def __init__(self, model_path='../../models/injection_molding_failure_prediction_model.pkl'):
        """
        Initialize the predictor with a trained model
        
        Args:
            model_path (str): Path to the saved model file
        """
        # Load the trained model
        self.model = joblib.load(model_path)
        
        # Load the feature columns used during training
        try:
            with open('feature_columns.txt', 'r') as f:
                self.feature_columns = [line.strip() for line in f.readlines()]
        except FileNotFoundError:
            print("Warning: feature_columns.txt not found. Prediction may not work correctly.")
            self.feature_columns = []
        
        # Store the most recent reading for each machine to calculate changes
        self.previous_readings = {}
    
    def prepare_data_point(self, sensor_data):
        """
        Prepare a single data point for prediction
        
        Args:
            sensor_data (dict): Sensor data from the injection molding machine
        
        Returns:
            pd.DataFrame: Prepared features for prediction
        """
        machine_id = sensor_data.get('machine_id', '')
        readings = sensor_data.get('readings', {})
        
        # Extract basic features from the sensor data
        data_point = {
            'barrel_temperature_celsius': readings.get('barrel_temperature_celsius', 0),
            'mold_temperature_celsius': readings.get('mold_temperature_celsius', 0),
            'injection_pressure_bar': readings.get('injection_pressure_bar', 0),
            'holding_pressure_bar': readings.get('holding_pressure_bar', 0),
            'injection_speed_mm_per_s': readings.get('injection_speed_mm_per_s', 0),
            'cooling_time_s': readings.get('cooling_time_s', 0),
            'cycle_time_s': readings.get('cycle_time_s', 0),
            'material_viscosity_pa_s': readings.get('material_viscosity_pa_s', 0),
            'mold_clamping_force_tons': readings.get('mold_clamping_force_tons', 0),
            'operation_time_hours': readings.get('operation_time_hours', 0),
            'parts_produced': readings.get('parts_produced', 0),
            'mold_wear_percent': readings.get('mold_wear_percent', 0),
            'time_since_maintenance': readings.get('time_since_maintenance', 0),
            'hour_of_day': datetime.now().hour,
            'day_of_week': datetime.now().weekday()
        }
        
        # Add derived features
        data_point['temp_ratio'] = data_point['barrel_temperature_celsius'] / max(data_point['mold_temperature_celsius'], 0.1)
        data_point['pressure_ratio'] = data_point['injection_pressure_bar'] / max(data_point['holding_pressure_bar'], 0.1)
        data_point['wear_per_hour'] = data_point['mold_wear_percent'] / max(data_point['operation_time_hours'], 0.1)
        data_point['parts_per_hour'] = data_point['parts_produced'] / max(data_point['operation_time_hours'], 0.1)
        data_point['cycle_efficiency'] = 3600 / max(data_point['cycle_time_s'], 0.1)  # Cycles per hour
        
        # Create a dataframe for this data point
        df = pd.DataFrame([data_point])
        
        # Add previous readings and changes if available
        prev_features = [
            'barrel_temperature_celsius', 'mold_temperature_celsius', 
            'injection_pressure_bar', 'holding_pressure_bar', 
            'injection_speed_mm_per_s', 'material_viscosity_pa_s', 
            'mold_wear_percent'
        ]
        
        if machine_id in self.previous_readings:
            prev = self.previous_readings[machine_id]
            for feature in prev_features:
                df[f'{feature}_prev'] = prev.get(feature, df[feature].values[0])
                df[f'{feature}_change'] = df[feature].values[0] - prev.get(feature, df[feature].values[0])
        else:
            # If no previous reading, use current values (no change)
            for feature in prev_features:
                df[f'{feature}_prev'] = df[feature]
                df[f'{feature}_change'] = 0
        
        # Store the current reading for future predictions
        self.previous_readings[machine_id] = {feature: df[feature].values[0] for feature in prev_features}
        
        # Ensure all required features are present
        for feature in self.feature_columns:
            if feature not in df.columns:
                df[feature] = 0
        
        # Select only the features used during training, in the same order
        return df[self.feature_columns]
    
    def predict_failure_probability(self, sensor_data):
        """
        Predict the probability of machine failure
        
        Args:
            sensor_data (dict): Sensor data from the injection molding machine
        
        Returns:
            dict: Prediction results including failure probability and risk level
        """
        # Prepare the data point for prediction
        features = self.prepare_data_point(sensor_data)
        
        # Predict failure probability
        failure_prob = self.model.predict_proba(features)[0][1]
        
        # Determine risk level
        if failure_prob < 0.3:
            risk_level = "LOW"
        elif failure_prob < 0.7:
            risk_level = "MEDIUM"
        else:
            risk_level = "HIGH"
        
        # Create the prediction result
        result = {
            "machine_id": sensor_data.get('machine_id', 'unknown'),
            "failure_probability": round(failure_prob, 3),
            "risk_level": risk_level,
            "prediction_timestamp": datetime.now().isoformat()
        }
        
        return result
    
if __name__ == "__main__":

    predictor = InjectionMoldingPredictor()

    sensor_data = {
        "timestamp": datetime.now(),
        "machine_id": "INJ-MOLD-78f8c6",
        "machine_type": "INJECTION_MOLDING",
        "readings": {
            "barrel_temperature_celsius": 215.86,
            "mold_temperature_celsius": 42.2,
            "injection_pressure_bar": 1173.36,
            "holding_pressure_bar": 768.75,
            "injection_speed_mm_per_s": 86.51,
            "cooling_time_s": 15.5,
            "cycle_time_s": 35.79,
            "material_viscosity_pa_s": 322.51,
            "mold_clamping_force_tons": 237.91,
            "operation_time_hours": 0.12,
            "parts_produced": 12,
            "mold_wear_percent": -3.91
        }
    }

    result = predictor.predict_failure_probability(sensor_data)
    print(result)