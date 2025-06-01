import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score, f1_score, precision_score, recall_score
from sklearn.pipeline import Pipeline
import xgboost as xgb
import joblib
import os

class CNCFailurePredictionTrainer:
    def __init__(self, dataset_path='../../dataset/cnc_machine_dataset.csv'):
        """
        Initialize the failure prediction model trainer
        
        Args:
            dataset_path (str): Path to the CSV dataset
        """
        self.dataset_path = dataset_path
        self.X = None
        self.y = None
        self.df = None
        self.best_model = None
        
        # Create directories for outputs
        os.makedirs('models', exist_ok=True)
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
        
        # Extract additional time-based features
        self.df['hour_of_day'] = self.df['timestamp'].dt.hour
        self.df['day_of_week'] = self.df['timestamp'].dt.dayofweek
        
        # Add some engineered features
        self.df['speed_temp_ratio'] = self.df['spindle_speed_rpm'] / self.df['motor_temperature_celsius']
        self.df['wear_per_hour'] = self.df['tool_wear_percent'] / np.maximum(self.df['operation_time_hours'], 0.1)
        self.df['coolant_efficiency'] = self.df['coolant_flow_rate_l_min'] / np.maximum(self.df['coolant_temperature_celsius'], 0.1)
        self.df['vibration_per_speed'] = self.df['vibration_mm_per_s'] * 1000 / np.maximum(self.df['spindle_speed_rpm'], 0.1)
        
        # Create lagged features for each machine (previous reading)
        lag_features = [
            'spindle_speed_rpm', 'cutting_force_n', 'motor_temperature_celsius', 
            'coolant_temperature_celsius', 'coolant_flow_rate_l_min', 'vibration_mm_per_s',
            'tool_wear_percent'
        ]
        
        # Group by machine_id and create lagged features
        for feature in lag_features:
            self.df[f'{feature}_prev'] = self.df.groupby('machine_id')[feature].shift(1)
            self.df[f'{feature}_change'] = self.df[feature] - self.df[f'{feature}_prev']
        
        # Drop rows with NaN values (from lag features)
        self.df = self.df.dropna()
        
        # Select features for modeling
        feature_columns = [
            'spindle_speed_rpm', 'cutting_force_n', 'motor_temperature_celsius', 
            'coolant_temperature_celsius', 'coolant_flow_rate_l_min', 'vibration_mm_per_s',
            'tool_wear_percent', 'operation_time_hours', 'parts_produced', 
            'time_since_maintenance', 'hour_of_day', 'day_of_week',
            'speed_temp_ratio', 'wear_per_hour', 'coolant_efficiency', 'vibration_per_speed'
        ] + [f'{feature}_prev' for feature in lag_features] + [f'{feature}_change' for feature in lag_features]
        
        # Prepare features and target
        self.X = self.df[feature_columns]
        self.y = self.df['failure']
        print(f"Features used: {feature_columns}")
        print(f"Dataset shape: {self.X.shape}")
        
        # Save feature list for prediction
        joblib.dump(feature_columns, 'models/feature_columns.pkl')
        
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
        plt.figure(figsize=(14, 12))
        key_features = [
            'spindle_speed_rpm', 'cutting_force_n', 'motor_temperature_celsius', 
            'coolant_temperature_celsius', 'coolant_flow_rate_l_min', 'vibration_mm_per_s',
            'tool_wear_percent', 'time_since_maintenance', 'failure'
        ]
        correlation = self.df[key_features].corr()
        sns.heatmap(correlation, annot=True, cmap='coolwarm', fmt='.2f')
        plt.title('Correlation Matrix of Key Features')
        plt.tight_layout()
        plt.savefig('plots/correlation_matrix.png')
        
        # Feature distributions by failure status
        plt.figure(figsize=(18, 14))
        key_features = key_features[:-1]  # Exclude 'failure' for this plot
        
        for i, feature in enumerate(key_features):
            plt.subplot(3, 3, i+1)
            sns.histplot(data=self.df, x=feature, hue='failure', kde=True, bins=30, element='step')
            plt.title(f'Distribution of {feature} by Failure Status')
        
        plt.tight_layout()
        plt.savefig('plots/feature_distributions.png')
        
        # Failure rate by time of day and day of week
        plt.figure(figsize=(15, 6))
        plt.subplot(1, 2, 1)
        hour_failure = self.df.groupby('hour_of_day')['failure'].mean() * 100
        sns.barplot(x=hour_failure.index, y=hour_failure.values)
        plt.title('Failure Rate by Hour of Day')
        plt.ylabel('Failure Rate (%)')
        plt.xlabel('Hour of Day')
        
        plt.subplot(1, 2, 2)
        day_failure = self.df.groupby('day_of_week')['failure'].mean() * 100
        sns.barplot(x=day_failure.index, y=day_failure.values)
        plt.title('Failure Rate by Day of Week')
        plt.ylabel('Failure Rate (%)')
        plt.xlabel('Day of Week (0=Monday, 6=Sunday)')
        plt.tight_layout()
        plt.savefig('plots/time_based_failure_rate.png')
        
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
                    'classifier__max_depth': [None, 15, 25],
                    'classifier__min_samples_split': [2, 5],
                    'classifier__class_weight': [None, 'balanced']
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
                    'classifier__learning_rate': [0.1, 0.05],
                    'classifier__scale_pos_weight': [1, 3, 5]
                }
            ),
            'Gradient Boosting': (
                Pipeline([
                    ('scaler', StandardScaler()),
                    ('classifier', GradientBoostingClassifier(random_state=42))
                ]),
                {
                    'classifier__n_estimators': [100, 200],
                    'classifier__learning_rate': [0.1, 0.05],
                    'classifier__max_depth': [3, 6],
                    'classifier__subsample': [0.8, 1.0]
                }
            )
        }
        
        best_models = {}
        results = []
        
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
            precision = precision_score(y_test, y_pred)
            recall = recall_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)
            
            results.append({
                'model': name,
                'accuracy': accuracy,
                'precision': precision,
                'recall': recall,
                'f1_score': f1,
                'best_params': grid_search.best_params_
            })
            
            print(f"\n{name} - Best Parameters: {grid_search.best_params_}")
            print(f"{name} - Accuracy: {accuracy:.4f}, Precision: {precision:.4f}, Recall: {recall:.4f}, F1: {f1:.4f}")
            print(f"\n{name} - Classification Report:")
            print(classification_report(y_test, y_pred))
            
            # Confusion Matrix
            plt.figure(figsize=(8, 6))
            cm = confusion_matrix(y_test, y_pred)
            sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                        xticklabels=['No Failure', 'Failure'],
                        yticklabels=['No Failure', 'Failure'])
            plt.title(f'Confusion Matrix - {name}')
            plt.ylabel('True Label')
            plt.xlabel('Predicted Label')
            plt.tight_layout()
            plt.savefig(f'plots/{name.lower().replace(" ", "_")}_confusion_matrix.png')
            
            # Feature importance (if supported)
            if hasattr(best_model['classifier'], 'feature_importances_'):
                plt.figure(figsize=(12, 10))
                feature_importances = best_model['classifier'].feature_importances_
                feature_names = self.X.columns
                
                # Create a DataFrame for easier sorting and plotting
                importance_df = pd.DataFrame({
                    'Feature': feature_names,
                    'Importance': feature_importances
                }).sort_values('Importance', ascending=False)
                
                # Plot top 20 features
                top_features = importance_df.head(20)
                sns.barplot(x='Importance', y='Feature', data=top_features)
                plt.title(f'Top 20 Feature Importances - {name}')
                plt.tight_layout()
                plt.savefig(f'plots/{name.lower().replace(" ", "_")}_feature_importance.png')
        
        # Compare model performances
        results_df = pd.DataFrame(results)
        plt.figure(figsize=(14, 8))
        metrics = ['accuracy', 'precision', 'recall', 'f1_score']
        results_melted = pd.melt(results_df, id_vars=['model'], value_vars=metrics, 
                                 var_name='Metric', value_name='Value')
        sns.barplot(x='model', y='Value', hue='Metric', data=results_melted)
        plt.title('Model Performance Comparison')
        plt.ylim(0, 1)
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig('plots/model_comparison.png')
        
        # Select best model based on F1 score
        best_model_name = results_df.loc[results_df['f1_score'].idxmax(), 'model']
        self.best_model = best_models[best_model_name]
        
        # Save best model
        joblib.dump(self.best_model, '../../models/cnc_failure_prediction_model.pkl')
        
        # Save model details
        with open('models/model_details.txt', 'w') as f:
            f.write(f"Best Model: {best_model_name}\n")
            f.write(f"Best Parameters: {results_df.loc[results_df['f1_score'].idxmax(), 'best_params']}\n")
            f.write(f"Performance Metrics:\n")
            f.write(f"  Accuracy: {results_df.loc[results_df['f1_score'].idxmax(), 'accuracy']:.4f}\n")
            f.write(f"  Precision: {results_df.loc[results_df['f1_score'].idxmax(), 'precision']:.4f}\n")
            f.write(f"  Recall: {results_df.loc[results_df['f1_score'].idxmax(), 'recall']:.4f}\n")
            f.write(f"  F1 Score: {results_df.loc[results_df['f1_score'].idxmax(), 'f1_score']:.4f}\n")
        
        print(f"\nBest model ({best_model_name}) saved to 'models/cnc_failure_prediction_model.pkl'")
        print(f"Model details saved to 'models/model_details.txt'")
        
        return best_models, results_df
    
    def run_full_pipeline(self):
        """
        Run the entire machine learning pipeline
        
        Steps:
        1. Load and prepare data
        2. Perform exploratory data analysis
        3. Train and evaluate models
        """
        # Load and prepare data
        self.load_and_prepare_data()
        
        # Perform exploratory data analysis
        self.perform_exploratory_data_analysis()
        
        # Train and evaluate models
        best_models, results = self.train_and_evaluate_models()
        
        return best_models, results

if __name__ == "__main__":
    # Run the full pipeline
    trainer = CNCFailurePredictionTrainer('../../dataset/cnc_machine_dataset.csv')
    best_models, results = trainer.run_full_pipeline()
    
    print("\nTraining Complete!")
    print("\nModel Comparison:")
    print(results[['model', 'accuracy', 'precision', 'recall', 'f1_score']])