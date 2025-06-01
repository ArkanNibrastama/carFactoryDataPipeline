import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, f1_score
from sklearn.pipeline import Pipeline
import xgboost as xgb
import joblib
import os

class InjectionMoldingFailurePrediction:
    def __init__(self, dataset_path='../../dataset/injection_molding_dataset.csv'):
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
        
        # Create output directory for plots if it doesn't exist
        os.makedirs('plots', exist_ok=True)
    
    def load_and_prepare_data(self):
        """Load and prepare the dataset for modeling"""
        print("Loading and preparing data...")
        
        # Load the dataset
        self.df = pd.read_csv(self.dataset_path)
        
        # Convert timestamp to datetime
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
        
        # Create binary target variable: 1 for FAILURE, 0 for other statuses
        self.df['failure'] = self.df['status'].apply(lambda x: 1 if x == 'FAILURE' else 0)
        
        # Extract additional time-based features if not already present
        if 'hour_of_day' not in self.df.columns:
            self.df['hour_of_day'] = self.df['timestamp'].dt.hour
        if 'day_of_week' not in self.df.columns:
            self.df['day_of_week'] = self.df['timestamp'].dt.dayofweek
        
        # Add engineered features
        self.df['temp_ratio'] = self.df['barrel_temperature_celsius'] / self.df['mold_temperature_celsius']
        self.df['pressure_ratio'] = self.df['injection_pressure_bar'] / self.df['holding_pressure_bar']
        self.df['wear_per_hour'] = self.df['mold_wear_percent'] / np.maximum(self.df['operation_time_hours'], 0.1)
        self.df['parts_per_hour'] = self.df['parts_produced'] / np.maximum(self.df['operation_time_hours'], 0.1)
        self.df['cycle_efficiency'] = 3600 / np.maximum(self.df['cycle_time_s'], 0.1)  # Cycles per hour
        
        # Create lagged features for each machine (previous reading)
        lag_features = [
            'barrel_temperature_celsius', 'mold_temperature_celsius', 
            'injection_pressure_bar', 'holding_pressure_bar', 
            'injection_speed_mm_per_s', 'material_viscosity_pa_s', 
            'mold_wear_percent'
        ]
        
        # Group by machine_id and create lagged features
        for feature in lag_features:
            self.df[f'{feature}_prev'] = self.df.groupby('machine_id')[feature].shift(1)
            self.df[f'{feature}_change'] = self.df[feature] - self.df[f'{feature}_prev']
        
        # Drop rows with NaN values (from lag features)
        self.df = self.df.dropna()
        
        # Select features for modeling
        feature_columns = [
            'barrel_temperature_celsius', 'mold_temperature_celsius', 
            'injection_pressure_bar', 'holding_pressure_bar', 
            'injection_speed_mm_per_s', 'cooling_time_s', 
            'cycle_time_s', 'material_viscosity_pa_s', 
            'mold_clamping_force_tons', 'operation_time_hours', 
            'parts_produced', 'mold_wear_percent', 
            'time_since_maintenance', 'hour_of_day', 'day_of_week',
            'temp_ratio', 'pressure_ratio', 'wear_per_hour',
            'parts_per_hour', 'cycle_efficiency'
        ] + [f'{feature}_prev' for feature in lag_features] + [f'{feature}_change' for feature in lag_features]
        
        # Prepare features and target
        self.X = self.df[feature_columns]
        self.y = self.df['failure']
        
        # Save the feature list for prediction
        with open('feature_columns.txt', 'w') as f:
            for feature in feature_columns:
                f.write(f"{feature}\n")
        
        print(f"Data prepared successfully. Features: {len(feature_columns)}, Samples: {len(self.X)}")
        
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
        plt.savefig('plots/status_distribution.png')
        
        # Correlation matrix of key features
        plt.figure(figsize=(12, 10))
        key_features = [
            'barrel_temperature_celsius', 'mold_temperature_celsius', 
            'injection_pressure_bar', 'holding_pressure_bar', 
            'material_viscosity_pa_s', 'mold_wear_percent', 
            'time_since_maintenance', 'failure'
        ]
        correlation = self.df[key_features].corr()
        sns.heatmap(correlation, annot=True, cmap='coolwarm', fmt='.2f')
        plt.title('Correlation Matrix of Key Features')
        plt.tight_layout()
        plt.savefig('plots/correlation_matrix.png')
        
        # Feature distributions by failure status
        plt.figure(figsize=(16, 12))
        key_features = key_features[:-1]  # Exclude 'failure' for this plot
        
        for i, feature in enumerate(key_features):
            plt.subplot(2, 4, i+1)
            sns.histplot(data=self.df, x=feature, hue='failure', kde=True, bins=30, element='step')
            plt.title(f'Distribution of {feature} by Failure Status')
        
        plt.tight_layout()
        plt.savefig('plots/feature_distributions.png')
        
        # Plot relationships between key variables
        plt.figure(figsize=(16, 12))
        plt.subplot(2, 2, 1)
        sns.scatterplot(data=self.df, x='operation_time_hours', y='mold_wear_percent', hue='failure')
        plt.title('Mold Wear vs. Operation Time')
        
        plt.subplot(2, 2, 2)
        sns.scatterplot(data=self.df, x='injection_pressure_bar', y='material_viscosity_pa_s', hue='failure')
        plt.title('Material Viscosity vs. Injection Pressure')
        
        plt.subplot(2, 2, 3)
        sns.scatterplot(data=self.df, x='barrel_temperature_celsius', y='injection_speed_mm_per_s', hue='failure')
        plt.title('Injection Speed vs. Barrel Temperature')
        
        plt.subplot(2, 2, 4)
        sns.scatterplot(data=self.df, x='time_since_maintenance', y='cycle_time_s', hue='failure')
        plt.title('Cycle Time vs. Time Since Maintenance')
        
        plt.tight_layout()
        plt.savefig('plots/feature_relationships.png')
        
        print("EDA plots saved to 'plots' directory.")
    
    def train_and_evaluate_models(self):
        """Train and evaluate multiple models for failure prediction"""
        if self.X is None or self.y is None:
            raise ValueError("Data not prepared. Call load_and_prepare_data() first.")
        
        print("\nTraining and evaluating machine learning models...")
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            self.X, self.y, test_size=0.25, random_state=42, stratify=self.y
        )
        
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
                    ('classifier', xgb.XGBClassifier(random_state=42, eval_metric='logloss'))
                ]),
                {
                    'classifier__n_estimators': [100, 200],
                    'classifier__max_depth': [3, 6, 9],
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
        
        results = {}
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
            
            # Calculate metrics
            accuracy = accuracy_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)
            results[name] = {'accuracy': accuracy, 'f1': f1}
            
            print(f"\n{name} - Best Parameters: {grid_search.best_params_}")
            print(f"{name} - Accuracy: {accuracy:.4f}, F1 Score: {f1:.4f}")
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
            plt.savefig(f'plots/{name.lower().replace(" ", "_")}_confusion_matrix.png')
            
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
                plt.savefig(f'plots/{name.lower().replace(" ", "_")}_feature_importance.png')
        
        # Select the best model based on F1 score
        best_model_name = max(results.items(), key=lambda x: x[1]['f1'])[0]
        self.best_model = best_models[best_model_name]
        
        # Save the best model
        joblib.dump(self.best_model, '../../models/injection_molding_failure_prediction_model.pkl')
        print(f"\nBest model ({best_model_name}) saved to 'injection_molding_failure_prediction_model.pkl'")
        
        # Save model metadata
        with open('model_metadata.txt', 'w') as f:
            f.write(f"Best model: {best_model_name}\n")
            f.write(f"Accuracy: {results[best_model_name]['accuracy']:.4f}\n")
            f.write(f"F1 Score: {results[best_model_name]['f1']:.4f}\n")
            f.write(f"Training completed: {datetime.now().isoformat()}\n")
        
        return best_models, results
    
    def run_full_pipeline(self):
        """
        Run the entire machine learning pipeline
        
        Steps:
        1. Load and prepare data
        2. Perform exploratory data analysis
        3. Train and evaluate models
        4. Return best model and results
        """
        # Load and prepare data
        self.load_and_prepare_data()
        
        # Perform exploratory data analysis
        self.perform_exploratory_data_analysis()
        
        # Train and evaluate models
        best_models, results = self.train_and_evaluate_models()
        
        return best_models, results

if __name__ == "__main__":
    # Create an instance of the InjectionMoldingFailurePrediction class
    predictor = InjectionMoldingFailurePrediction('../../dataset/injection_molding_dataset.csv')
    
    # Run the full pipeline
    best_models, results = predictor.run_full_pipeline()
    
    print("\nModel training complete!")