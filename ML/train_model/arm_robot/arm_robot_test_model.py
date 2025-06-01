from datetime import datetime
import pandas as pd
import numpy as np
import pickle
import joblib

def predict_failure_probability(sensor_data, model):

        readings = sensor_data.get('readings', {})

        # Prepare data point for prediction
        data_point = {
            'servo_motor_rpm': readings.get('servo_motor_rpm', 0),
            'hydraulic_pressure_psi': readings.get('hydraulic_pressure_psi', 0),
            'temperature_celsius': readings.get('temperature_celsius', 0),
            'vibration_mm_per_s': readings.get('vibration_mm_per_s', 0),
            'operation_time_hours': readings.get('operation_time_hours', 0),
            'cycle_count': readings.get('cycle_count', 0),
            'joint_wear_percent': readings.get('joint_wear_percent', 0),
            
            # Add default values for additional required features
            'time_since_maintenance': 0,
            'hour_of_day': datetime.now().hour,
            'day_of_week': datetime.now().weekday()
        }
        
        # Convert the data point to a DataFrame with expected feature names
        features = pd.DataFrame([data_point])
        
        # Add engineered features similar to training
        features['pressure_temp_ratio'] = features['hydraulic_pressure_psi'] / features['temperature_celsius']
        features['wear_per_hour'] = features['joint_wear_percent'] / np.maximum(features['operation_time_hours'], 0.1)
        features['rpm_vibration_ratio'] = features['servo_motor_rpm'] / np.maximum(features['vibration_mm_per_s'], 0.1)
        
        column_list = ['servo_motor_rpm', 'hydraulic_pressure_psi', 'temperature_celsius', 
                       'vibration_mm_per_s', 'operation_time_hours', 'cycle_count', 'joint_wear_percent', 
                       'time_since_maintenance', 'hour_of_day', 'day_of_week', 'pressure_temp_ratio', 
                       'wear_per_hour', 'rpm_vibration_ratio', 'servo_motor_rpm_prev', 
                       'hydraulic_pressure_psi_prev', 'temperature_celsius_prev', 'vibration_mm_per_s_prev', 
                       'joint_wear_percent_prev', 'servo_motor_rpm_change', 'hydraulic_pressure_psi_change', 
                       'temperature_celsius_change', 'vibration_mm_per_s_change', 'joint_wear_percent_change']

        # Align features with the model's expected format
        required_features = column_list
        missing_features = set(required_features) - set(features.columns)
        
        # Initialize missing features with zero or appropriate values
        for feature in missing_features:
            if '_prev' in feature:
                base_feature = feature.replace('_prev', '')
                if base_feature in features.columns:
                    features[feature] = features[base_feature]
                else:
                    features[feature] = 0
            elif '_change' in feature:
                features[feature] = 0
            else:
                features[feature] = 0
        
        # Select features in the same order as during training
        features_for_prediction = features[required_features]
        
        # Predict probability
        failure_prob = model.predict_proba(features_for_prediction)[0][1]
        
        # Determine risk level
        if failure_prob < 0.3:
            risk_level = "LOW"
        elif failure_prob < 0.7:
            risk_level = "MEDIUM"
        else:
            risk_level = "HIGH"
        
        return {
            "failure_probability": round(failure_prob, 3),
            "risk_level": risk_level,
            "prediction_timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    # with open("robotic_arm_failure_prediction_model.pkl", "rb") as file:
    #     model = pickle.load(file)
    model = joblib.load("../../models/robotic_arm_failure_prediction_model.pkl")

   # Example prediction
    sample_sensor_data = {
        "timestamp": datetime.now(),
        "machine_id": "ROBOT_ARM_001",
        "machine_type": "ROBOTIC_ARM",
        "readings": {
            "servo_motor_rpm": 1250.0,
            "hydraulic_pressure_psi": 2050.0,
            "temperature_celsius": 72.0,
            "vibration_mm_per_s": 0.65,
            "operation_time_hours": 120.0,
            "cycle_count": 1200,
            "joint_wear_percent": 35.0
        }
    }

    # Make a sample prediction
    prediction = predict_failure_probability(sample_sensor_data, model)
    print("\nSample Prediction:")
    print(prediction)