import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
from sklearn.pipeline import Pipeline
import xgboost as xgb
import joblib

class RoboticArmFailurePrediction:
    def __init__(self, dataset_path='../../dataset/robotic_arm_dataset.csv'):
        """
        Initialize the failure prediction model
        
        Args:
            dataset_path (str): Path to the CSV dataset
        """
        self.dataset_path = dataset_path
        self.X = None
        self.y = None
        self.df = None
        self.best_model = None
    
    def load_and_prepare_data(self):
        """Load and prepare the dataset for modeling"""
        print("Loading and preparing data...")
        
        # Load the dataset
        self.df = pd.read_csv(self.dataset_path)
        
        # Convert timestamp to datetime
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        
        # Create binary target variable: 1 for FAILURE, 0 for other statuses
        self.df['failure'] = self.df['status'].apply(lambda x: 1 if x == 'FAILURE' else 0)
        
        # Extract additional time-based features
        self.df['hour_of_day'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.dayofweek
        
        # Add some engineered features
        self.df['pressure_temp_ratio'] = self.df['hydraulic_pressure_psi'] / self.df['temperature_celsius']
        self.df['wear_per_hour'] = self.df['joint_wear_percent'] / np.maximum(self.df['operation_time_hours'], 0.1)
        self.df['rpm_vibration_ratio'] = self.df['servo_motor_rpm'] / np.maximum(self.df['vibration_mm_per_s'], 0.1)
        
        # Create lagged features for each machine (previous reading)
        lag_features = [
            'servo_motor_rpm', 'hydraulic_pressure_psi', 'temperature_celsius', 
            'vibration_mm_per_s', 'joint_wear_percent'
        ]
        
        # Group by machine_id and create lagged features
        for feature in lag_features:
            self.df[f'{feature}_prev'] = self.df.groupby('machine_id')[feature].shift(1)
            self.df[f'{feature}_change'] = self.df[feature] - self.df[f'{feature}_prev']
        
        # Drop rows with NaN values (from lag features)
        self.df = self.df.dropna()
        
        # Select features for modeling
        feature_columns = [
            'servo_motor_rpm', 'hydraulic_pressure_psi', 'temperature_celsius', 
            'vibration_mm_per_s', 'operation_time_hours', 'cycle_count', 
            'joint_wear_percent', 'time_since_maintenance', 'hour_of_day', 
            'day_of_week', 'pressure_temp_ratio', 'wear_per_hour', 
            'rpm_vibration_ratio'
        ] + [f'{feature}_prev' for feature in lag_features] + [f'{feature}_change' for feature in lag_features]
        
        

        # Prepare features and target
        self.X = self.df[feature_columns]
        self.y = self.df['failure']
        print(f"this is X columns = {feature_columns}")
        
        return self.X, self.y, self.df
    
    def perform_exploratory_data_analysis(self):
        """Perform exploratory data analysis on the dataset"""
        if self.df is None:
            raise ValueError("Data not loaded. Call load_and_prepare_data() first.")
        
        print("\nPerforming Exploratory Data Analysis...")
        
        # Status distribution
        plt.figure(figsize=(10, 6))
        status_counts = self.df['status'].value_counts()
        sns.barplot(x=status_counts.index, y=status_counts.values)
        plt.title('Distribution of Machine Statuses')
        plt.ylabel('Count')
        plt.xlabel('Status')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig('status_distribution.png')
        
        # Correlation matrix of key features
        plt.figure(figsize=(12, 10))
        key_features = [
            'servo_motor_rpm', 'hydraulic_pressure_psi', 'temperature_celsius', 
            'vibration_mm_per_s', 'joint_wear_percent', 'time_since_maintenance',
            'failure'
        ]
        correlation = self.df[key_features].corr()
        sns.heatmap(correlation, annot=True, cmap='coolwarm', fmt='.2f')
        plt.title('Correlation Matrix of Key Features')
        plt.tight_layout()
        plt.savefig('correlation_matrix.png')
        
        # Feature distributions by failure status
        plt.figure(figsize=(16, 12))
        key_features = key_features[:-1]  # Exclude 'failure' for this plot
        
        for i, feature in enumerate(key_features):
            plt.subplot(2, 3, i+1)
            sns.histplot(data=self.df, x=feature, hue='failure', kde=True, bins=30, element='step')
            plt.title(f'Distribution of {feature} by Failure Status')
        
        plt.tight_layout()
        plt.savefig('feature_distributions.png')
        
        print("EDA plots saved to disk.")
    
    def train_and_evaluate_models(self):
        """Train and evaluate multiple models for failure prediction"""
        if self.X is None or self.y is None:
            raise ValueError("Data not prepared. Call load_and_prepare_data() first.")
        
        print("\nTraining and evaluating machine learning models...")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(self.X, self.y, test_size=0.25, random_state=42, stratify=self.y)
        
        # Model pipelines
        pipelines = {
            'Random Forest': (
                Pipeline([
                    ('scaler', StandardScaler()),
                    ('classifier', RandomForestClassifier(random_state=42))
                ]),
                {
                    'classifier__n_estimators': [100, 200],
                    'classifier__max_depth': [None, 10, 20],
                    'classifier__min_samples_split': [2, 5]
                }
            ),
            'XGBoost': (
                Pipeline([
                    ('scaler', StandardScaler()),
                    ('classifier', xgb.XGBClassifier(random_state=42, use_label_encoder=False, eval_metric='logloss'))
                ]),
                {
                    'classifier__n_estimators': [100, 200],
                    'classifier__max_depth': [3, 6],
                    'classifier__learning_rate': [0.1, 0.01]
                }
            ),
            'Gradient Boosting': (
                Pipeline([
                    ('scaler', StandardScaler()),
                    ('classifier', GradientBoostingClassifier(random_state=42))
                ]),
                {
                    'classifier__n_estimators': [100, 200],
                    'classifier__learning_rate': [0.1, 0.01],
                    'classifier__max_depth': [3, 6]
                }
            )
        }
        
        best_models = {}
        
        # Train and evaluate each model
        for name, (pipeline, param_grid) in pipelines.items():
            print(f"\nTraining {name}...")
            
            # Grid search with cross-validation
            grid_search = GridSearchCV(
                pipeline, param_grid, cv=5, scoring='f1', n_jobs=-1, verbose=1
            )
            grid_search.fit(X_train, y_train)
            
            # Get best model
            best_model = grid_search.best_estimator_
            best_models[name] = best_model
            
            # Make predictions
            y_pred = best_model.predict(X_test)
            
            # Calculate and display metrics
            accuracy = accuracy_score(y_test, y_pred)
            
            print(f"\n{name} - Best Parameters: {grid_search.best_params_}")
            print(f"{name} - Accuracy: {accuracy:.4f}")
            print(f"\n{name} - Classification Report:")
            print(classification_report(y_test, y_pred))
            
            # Confusion Matrix
            plt.figure(figsize=(8, 6))
            cm = confusion_matrix(y_test, y_pred)
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues')
            plt.title(f'Confusion Matrix - {name}')
            plt.ylabel('True Label')
            plt.xlabel('Predicted Label')
            plt.tight_layout()
            plt.savefig(f'{name.lower().replace(" ", "_")}_confusion_matrix.png')
            
            # Feature importance (if supported)
            if hasattr(best_model['classifier'], 'feature_importances_'):
                plt.figure(figsize=(12, 8))
                feature_importances = best_model['classifier'].feature_importances_
                feature_names = self.X.columns
                
                # Create a DataFrame for easier sorting and plotting
                importance_df = pd.DataFrame({
                    'Feature': feature_names,
                    'Importance': feature_importances
                }).sort_values('Importance', ascending=False)
                
                # Plot top 15 features
                top_features = importance_df.head(15)
                sns.barplot(x='Importance', y='Feature', data=top_features)
                plt.title(f'Top 15 Feature Importances - {name}')
                plt.tight_layout()
                plt.savefig(f'{name.lower().replace(" ", "_")}_feature_importance.png')
        
        # Select and save the best model
        best_model_name = max(best_models.items(), key=lambda x: accuracy_score(y_test, x[1].predict(X_test)))[0]
        self.best_model = best_models[best_model_name]
        joblib.dump(self.best_model, '../../models/robotic_arm_failure_prediction_model.pkl')
        print(f"\nBest model ({best_model_name}) saved to 'robotic_arm_failure_prediction_model.pkl'")
        
        return best_models
    
    def run_full_pipeline(self):
        """
        Run the entire machine learning pipeline
        
        Steps:
        1. Load and prepare data
        2. Perform exploratory data analysis
        3. Train and evaluate models
        4. Return best model
        """
        # Load and prepare data
        self.load_and_prepare_data()
        
        # Perform exploratory data analysis
        self.perform_exploratory_data_analysis()
        
        # Train and evaluate models
        best_models = self.train_and_evaluate_models()
        
        return best_models

if __name__ == "__main__":
    # Create an instance of the RoboticArmFailurePrediction class
    predictor = RoboticArmFailurePrediction('../../dataset/robotic_arm_dataset.csv')
    
    # Run the full pipeline
    best_models = predictor.run_full_pipeline()
    


 
