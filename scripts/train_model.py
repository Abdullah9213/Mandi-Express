"""
Phase II: Model Training with MLflow Tracking
Trains the model and logs everything to DagsHub/MLflow.
"""
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib
from datetime import datetime

# MLflow imports
import mlflow
import mlflow.sklearn

# Configuration
DATA_PATH = os.getenv("DATA_PATH", "include/processed_combined.csv")
MODEL_PATH = os.getenv("MODEL_PATH", "models/mandi_pipeline.pkl")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "")
DAGSHUB_USERNAME = os.getenv("DAGSHUB_USERNAME", "")
DAGSHUB_TOKEN = os.getenv("DAGSHUB_TOKEN", "")

def setup_mlflow():
    """Configure MLflow with DagsHub."""
    if MLFLOW_TRACKING_URI and DAGSHUB_TOKEN:
        os.environ["MLFLOW_TRACKING_USERNAME"] = DAGSHUB_USERNAME
        os.environ["MLFLOW_TRACKING_PASSWORD"] = DAGSHUB_TOKEN
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        mlflow.set_experiment("mandi_model_training")
        print(f"MLflow configured: {MLFLOW_TRACKING_URI}")
        return True
    else:
        print("MLflow not configured. Training locally.")
        return False

def load_data(path):
    """Load and prepare data."""
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} rows from {path}")
    
    # Ensure required columns exist
    required = ['City', 'Crop', 'Year', 'Month', 'Day', 'Price']
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
    
    return df

def train_model(df, hyperparams=None):
    """Train RandomForest model with pipeline."""
    
    if hyperparams is None:
        hyperparams = {
            'n_estimators': 100,
            'max_depth': 10,
            'min_samples_split': 5,
            'random_state': 42
        }
    
    # Features and target
    X = df[['City', 'Crop', 'Year', 'Month', 'Day']]
    y = df['Price']
    
    # Train/test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Preprocessing
    categorical_features = ['City', 'Crop']
    numerical_features = ['Year', 'Month', 'Day']
    
    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features),
            ('num', StandardScaler(), numerical_features)
        ]
    )
    
    # Pipeline
    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('regressor', RandomForestRegressor(**hyperparams))
    ])
    
    # Train
    print("Training model...")
    pipeline.fit(X_train, y_train)
    
    # Evaluate
    y_pred = pipeline.predict(X_test)
    
    metrics = {
        'rmse': float(np.sqrt(mean_squared_error(y_test, y_pred))),
        'mae': float(mean_absolute_error(y_test, y_pred)),
        'r2': float(r2_score(y_test, y_pred)),
        'train_size': len(X_train),
        'test_size': len(X_test)
    }
    
    print(f"RMSE: {metrics['rmse']:.2f}")
    print(f"MAE: {metrics['mae']:.2f}")
    print(f"R2: {metrics['r2']:.4f}")
    
    return pipeline, metrics, hyperparams

def save_model(pipeline, path):
    """Save model to disk."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    joblib.dump(pipeline, path)
    print(f"Model saved: {path}")

def main():
    """Main training function."""
    print("=" * 60)
    print("MANDI PRICE PREDICTION - MODEL TRAINING")
    print("=" * 60)
    
    # Setup MLflow
    mlflow_enabled = setup_mlflow()
    
    # Load data
    df = load_data(DATA_PATH)
    
    # Hyperparameters (can be modified for experiments)
    hyperparams = {
        'n_estimators': 100,
        'max_depth': 10,
        'min_samples_split': 5,
        'random_state': 42
    }
    
    if mlflow_enabled:
        with mlflow.start_run(run_name=f"training_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
            # Log hyperparameters
            mlflow.log_params(hyperparams)
            mlflow.log_param("data_path", DATA_PATH)
            mlflow.log_param("data_rows", len(df))
            
            # Train
            pipeline, metrics, _ = train_model(df, hyperparams)
            
            # Log metrics
            mlflow.log_metrics(metrics)
            
            # Save and log model (for local storage)
            save_model(pipeline, MODEL_PATH)
            mlflow.log_artifact(MODEL_PATH)
            
            # Log the model as an artifact within the run.
            # NOTE: I removed the `registered_model_name` argument because the DagsHub endpoint 
            # for the MLflow Model Registry appears to be unsupported, which was causing the error.
            mlflow.sklearn.log_model(
                sk_model=pipeline,
                artifact_path="mandi_model",
                # registered_model_name="mandi_price_predictor" # Removed
            )
            
            print("Model logged as artifact in the MLflow run.")
    else:
        # Train without MLflow
        pipeline, metrics, _ = train_model(df, hyperparams)
        save_model(pipeline, MODEL_PATH)
    
    print("=" * 60)
    print("TRAINING COMPLETE!")
    print("=" * 60)
    
    return metrics

if __name__ == "__main__":
    main()