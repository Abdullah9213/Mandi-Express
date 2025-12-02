from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import os
import re
import json
import requests
import socket
import subprocess # Already used, just ensuring import is visible

# CONFIGURATION
DATA_PATH = "/usr/local/airflow/include/processed_combined.csv"
RAW_DATA_DIR = "/usr/local/airflow/include/raw_history"
URL = "http://www.amis.pk/daily%20market%20changes.aspx"

# DAGSHUB/MLFLOW CONFIG
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "https://dagshub.com/i222515/mandi.mlflow")
DAGSHUB_USERNAME = os.getenv("DAGSHUB_USERNAME", "i222515")
DAGSHUB_TOKEN = os.getenv("DAGSHUB_TOKEN", "")

# AWS S3 CONFIG (Mandatory for DVC Push)
# These environment variables must be securely set in your Airflow environment (e.g., in Airflow Connections or Docker environment).
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1") # Updated to Europe (Stockholm)

# MINIO CONFIG (If used for separate backup/storage)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "mandi-data")

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1), 
    catchup=False, 
    default_args=default_args,
    tags=['mandi_ai', 'production'],
    description="Phase I: Full-Scale Data Ingestion with Quality Gates, Profiling, and DVC"
)
def mandi_automation():

    def clean_crop_name(name):
        """Removes Urdu characters and extra parentheses."""
        if not isinstance(name, str):
            return str(name)
        clean_name = re.sub(r'[^\x00-\x7F]+', '', name)
        clean_name = re.sub(r'$$\s*$$', '', clean_name).strip()
        return clean_name

    def check_network_connectivity():
        """Check if we can reach external sites."""
        try:
            socket.setdefaulttimeout(10)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(("8.8.8.8", 53))
            return True
        except socket.error:
            return False

    # --- TASK 1: EXTRACTION ---
    @task()
    def extract_all_data():
        """Extract data from AMIS Pakistan and save raw data with timestamp."""
        
        if not check_network_connectivity():
            print("WARNING: No external network connectivity detected.")
            print("Falling back to local data if available...")
            
            if os.path.exists(DATA_PATH):
                existing_df = pd.read_csv(DATA_PATH)
                last_date = existing_df['Date'].max()
                print(f"Using existing data. Last date: {last_date}")
                
                latest_data = existing_df[existing_df['Date'] == last_date].copy()
                latest_data['Date'] = datetime.now().strftime("%Y-%m-%d")
                latest_data['ExtractionTimestamp'] = datetime.now().isoformat()
                
                return {
                    "data": latest_data.to_dict(orient='records'),
                    "raw_file": None,
                    "extraction_time": datetime.now().isoformat(),
                    "fallback": True
                }
            else:
                raise ConnectionError("No network and no local data available.")
        
        print(f"Connecting to {URL}...")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(URL, headers=headers, timeout=30)
            response.raise_for_status()
            
            from io import StringIO
            dfs = pd.read_html(StringIO(response.text))
            print(f"Found {len(dfs)} tables.")
            
            target_df = None
            for df in dfs:
                df.columns = [str(c).strip() for c in df.columns]
                if 'CityName' in df.columns and 'CropName' in df.columns:
                    target_df = df
                    break
            
            if target_df is None:
                if len(dfs) > 0:
                    target_df = max(dfs, key=len)
                    if target_df.shape[1] >= 3:
                        target_df.columns = ['CityName', 'CropName', "Today's FQP/Average Price"] + list(target_df.columns[3:])
                else:
                    raise ValueError("No tables found on page.")

            if 'CityName' in target_df.columns:
                target_df.rename(columns={
                    'CityName': 'City', 
                    'CropName': 'Crop', 
                    "Today's FQP/Average Price": 'Price'
                }, inplace=True)

            target_df['Price'] = target_df['Price'].astype(str).str.replace(',', '', regex=False)
            target_df['Price'] = pd.to_numeric(target_df['Price'], errors='coerce')
            
            if 'Crop' in target_df.columns:
                target_df['Crop'] = target_df['Crop'].apply(clean_crop_name)
            
            extraction_time = datetime.now()
            target_df['Date'] = extraction_time.strftime("%Y-%m-%d")
            target_df['ExtractionTimestamp'] = extraction_time.isoformat()
            
            final_df = target_df[['Date', 'City', 'Crop', 'Price', 'ExtractionTimestamp']]
            
            os.makedirs(RAW_DATA_DIR, exist_ok=True)
            raw_filename = f"raw_{extraction_time.strftime('%Y%m%d_%H%M%S')}.csv"
            raw_path = os.path.join(RAW_DATA_DIR, raw_filename)
            final_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
            print(f"Raw data saved to {raw_path}")
            
            print(f"SUCCESS: Extracted {len(final_df)} rows of data.")
            return {
                "data": final_df.to_dict(orient='records'),
                "raw_file": raw_path,
                "extraction_time": extraction_time.isoformat(),
                "fallback": False
            }

        except requests.exceptions.RequestException as e:
            print(f"Network Error: {e}")
            if os.path.exists(DATA_PATH):
                print("Falling back to local data...")
                existing_df = pd.read_csv(DATA_PATH)
                last_date = existing_df['Date'].max()
                latest_data = existing_df[existing_df['Date'] == last_date].copy()
                latest_data['Date'] = datetime.now().strftime("%Y-%m-%d")
                latest_data['ExtractionTimestamp'] = datetime.now().isoformat()
                
                return {
                    "data": latest_data.to_dict(orient='records'),
                    "raw_file": None,
                    "extraction_time": datetime.now().isoformat(),
                    "fallback": True
                }
            raise
        except Exception as e:
            print(f"Critical Scrape Error: {e}")
            raise

    # --- TASK 2: STRICT DATA QUALITY GATE ---
    @task()
    def strict_quality_gate(extraction_result: dict):
        """
        Mandatory Quality Gate:
        - Check for >1% null values in key columns
        - Schema validation
        - Fail DAG if quality check fails
        """
        data_list = extraction_result.get("data", [])
        
        if not data_list:
            raise ValueError("QUALITY GATE FAILED: No data extracted.")
        
        df = pd.DataFrame(data_list)
        total_rows = len(df)
        
        print(f"Running Quality Gate on {total_rows} records...")
        
        required_columns = ['Date', 'City', 'Crop', 'Price']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"QUALITY GATE FAILED: Missing columns: {missing_cols}")
        
        quality_report = {}
        for col in required_columns:
            null_count = df[col].isnull().sum()
            null_pct = (null_count / total_rows) * 100
            quality_report[col] = {
                "null_count": int(null_count),
                "null_percentage": round(null_pct, 2)
            }
            
            if null_pct > 1.0:
                raise ValueError(
                    f"QUALITY GATE FAILED: Column '{col}' has {null_pct:.2f}% null values (threshold: 1%)"
                )
        
        if not pd.api.types.is_numeric_dtype(df['Price']):
            raise ValueError("QUALITY GATE FAILED: 'Price' column is not numeric")
        
        invalid_prices = df[df['Price'] <= 0]
        if len(invalid_prices) > 0:
            invalid_pct = (len(invalid_prices) / total_rows) * 100
            if invalid_pct > 5.0:
                raise ValueError(f"QUALITY GATE FAILED: {invalid_pct:.2f}% of prices are invalid (<=0)")
        
        valid_df = df[df['Price'] > 0].dropna(subset=required_columns)
        
        print(f"QUALITY GATE PASSED: {len(valid_df)}/{total_rows} records valid")
        print(f"Quality Report: {json.dumps(quality_report, indent=2)}")
        
        return {
            "data": valid_df.to_dict(orient='records'),
            "raw_file": extraction_result.get("raw_file"),
            "extraction_time": extraction_result.get("extraction_time"),
            "quality_report": quality_report,
            "valid_count": len(valid_df),
            "total_count": total_rows
        }

    # --- TASK 3: FEATURE ENGINEERING ---
    @task()
    def transform_and_engineer_features(validated_result: dict):
        """
        Transform data and create features:
        - Time-based features (Year, Month, Day, DayOfWeek)
        - Lag features (previous prices)
        - Rolling means
        """
        df = pd.DataFrame(validated_result.get("data", []))
        
        df['Date'] = pd.to_datetime(df['Date'])
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month
        df['Day'] = df['Date'].dt.day
        df['DayOfWeek'] = df['Date'].dt.dayofweek
        df['WeekOfYear'] = df['Date'].dt.isocalendar().week.astype(int)
        df['IsWeekend'] = df['DayOfWeek'].isin([5, 6]).astype(int)
        
        if os.path.exists(DATA_PATH):
            historical_df = pd.read_csv(DATA_PATH)
            historical_df['Date'] = pd.to_datetime(historical_df['Date'])
            
            combined = pd.concat([historical_df, df], ignore_index=True)
            combined = combined.sort_values(['City', 'Crop', 'Date'])
            
            for lag in [1, 7]:
                combined[f'Price_Lag_{lag}'] = combined.groupby(['City', 'Crop'])['Price'].shift(lag)
            
            combined['Price_Rolling_7d'] = combined.groupby(['City', 'Crop'])['Price'].transform(
                lambda x: x.rolling(window=7, min_periods=1).mean()
            )
            
            df = combined[combined['Date'].isin(df['Date'].unique())].copy()
        else:
            df['Price_Lag_1'] = None
            df['Price_Lag_7'] = None
            df['Price_Rolling_7d'] = df['Price']
        
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        df = df.fillna('')  # Replace NaN with empty string for serialization
        
        print(f"Feature engineering complete. Columns: {list(df.columns)}")
        
        return {
            "data": df.to_dict(orient='records'),
            "raw_file": validated_result.get("raw_file"),
            "extraction_time": validated_result.get("extraction_time"),
            "quality_report": validated_result.get("quality_report"),
            "features_added": ['Year', 'Month', 'Day', 'DayOfWeek', 'WeekOfYear', 'IsWeekend', 
                              'Price_Lag_1', 'Price_Lag_7', 'Price_Rolling_7d']
        }

    # --- TASK 4: GENERATE PROFILING REPORT ---
    @task()
    def generate_profiling_report(transformed_result: dict):
        """
        Generate data profiling report using ydata-profiling (pandas-profiling).
        Log report as artifact to MLflow.
        """
        import mlflow
        
        df = pd.DataFrame(transformed_result.get("data", []))
        extraction_time = transformed_result.get("extraction_time", datetime.now().isoformat())
        
        os.environ["MLFLOW_TRACKING_USERNAME"] = DAGSHUB_USERNAME
        os.environ["MLFLOW_TRACKING_PASSWORD"] = DAGSHUB_TOKEN
        
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        
        try:
            mlflow.set_experiment("mandi_data_ingestion")
        except Exception as e:
            print(f"MLflow experiment setup warning: {e}")
            print("Continuing without MLflow tracking...")
            # Save report locally without MLflow
            report_dir = "/usr/local/airflow/include/reports"
            os.makedirs(report_dir, exist_ok=True)
            
            stats_report = {
                "shape": list(df.shape),
                "columns": list(df.columns),
                "record_count": len(df),
                "extraction_time": extraction_time
            }
            report_path = os.path.join(report_dir, f"stats_{extraction_time[:10]}.json")
            with open(report_path, 'w') as f:
                json.dump(stats_report, f, indent=2, default=str)
            
            return {
                "data": transformed_result.get("data"),
                "raw_file": transformed_result.get("raw_file"),
                "report_generated": True,
                "mlflow_logged": False
            }
        
        report_dir = "/usr/local/airflow/include/reports"
        os.makedirs(report_dir, exist_ok=True)
        
        with mlflow.start_run(run_name=f"data_ingestion_{extraction_time[:10]}"):
            mlflow.log_param("extraction_time", extraction_time)
            mlflow.log_param("record_count", len(df))
            mlflow.log_param("features", list(df.columns))
            
            quality_report = transformed_result.get("quality_report", {})
            for col, stats in quality_report.items():
                mlflow.log_metric(f"null_pct_{col}", stats.get("null_percentage", 0))
            
            try:
                from ydata_profiling import ProfileReport
                
                profile = ProfileReport(
                    df, 
                    title=f"Mandi Data Profile - {extraction_time[:10]}",
                    minimal=True, 
                    explorative=True
                )
                
                report_path = os.path.join(report_dir, f"profile_{extraction_time[:10]}.html")
                profile.to_file(report_path)
                
                mlflow.log_artifact(report_path, "profiling_reports")
                print(f"Profiling report saved and logged to MLflow: {report_path}")
                
            except ImportError:
                print("ydata-profiling not installed. Generating basic stats report...")
                
                stats_report = {
                    "shape": list(df.shape),
                    "columns": list(df.columns),
                    "dtypes": df.dtypes.astype(str).to_dict(),
                    "null_counts": df.isnull().sum().to_dict(),
                    "numeric_stats": df.describe().to_dict() if len(df.select_dtypes(include='number').columns) > 0 else {}
                }
                
                report_path = os.path.join(report_dir, f"stats_{extraction_time[:10]}.json")
                with open(report_path, 'w') as f:
                    json.dump(stats_report, f, indent=2, default=str)
                
                mlflow.log_artifact(report_path, "stats_reports")
                print(f"Basic stats report saved and logged to MLflow: {report_path}")
            
            mlflow.log_metric("ingestion_success", 1)
        
        return {
            "data": transformed_result.get("data"),
            "raw_file": transformed_result.get("raw_file"),
            "report_generated": True,
            "mlflow_logged": True
        }

    # --- TASK 5: LOAD AND VERSION WITH DVC ---
    @task()
    def load_version_and_store(profiled_result: dict):
        """
        Load processed data, version with DVC (using S3 credentials), and upload to MinIO.
        """
        
        df_new = pd.DataFrame(profiled_result.get("data", []))
        
        final_cols = ['City', 'Date', 'Crop', 'Price', 'Year', 'Month', 'Day', 
                      'DayOfWeek', 'WeekOfYear', 'IsWeekend', 
                      'Price_Lag_1', 'Price_Lag_7', 'Price_Rolling_7d']
        
        for col in final_cols:
            if col not in df_new.columns:
                df_new[col] = None
        
        df_new = df_new[final_cols]
        
        os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
        
        today_str = df_new['Date'].iloc[0] if len(df_new) > 0 else None
        
        # Data Appending Logic
        if os.path.exists(DATA_PATH) and today_str:
            existing_df = pd.read_csv(DATA_PATH)
            existing_df['Date'] = pd.to_datetime(existing_df['Date']).dt.strftime('%Y-%m-%d')
            
            if today_str in existing_df['Date'].values:
                print(f"Data for {today_str} already exists. Skipping append.")
                return {"status": "skipped", "reason": f"Duplicate for {today_str}"}
        
        if os.path.exists(DATA_PATH):
            df_new.to_csv(DATA_PATH, mode='a', header=False, index=False, encoding='utf-8-sig')
        else:
            df_new.to_csv(DATA_PATH, mode='w', header=True, index=False, encoding='utf-8-sig')
        
        print(f"Saved {len(df_new)} rows to {DATA_PATH}")
        
        # --- DVC Versioning and Push to S3 Remote ---
        try:
            dvc_dir = "/usr/local/airflow"
            
            # Prepare environment variables for DVC (AWS credentials required for S3 remote)
            dvc_env = os.environ.copy()
            dvc_env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
            dvc_env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
            dvc_env["AWS_REGION"] = AWS_REGION
            
            # 1. DVC Add
            result_add = subprocess.run(
                ["dvc", "add", DATA_PATH],
                cwd=dvc_dir,
                capture_output=True,
                text=True,
                env=dvc_env # Pass the environment variables
            )
            
            if result_add.returncode != 0:
                raise subprocess.CalledProcessError(
                    result_add.returncode, "dvc add", result_add.stdout, result_add.stderr
                )
            print(f"DVC add success: {result_add.stdout}")

            # 2. DVC Push
            result_push = subprocess.run(
                ["dvc", "push"],
                cwd=dvc_dir,
                capture_output=True,
                text=True,
                env=dvc_env # Pass the environment variables
            )

            if result_push.returncode != 0:
                raise subprocess.CalledProcessError(
                    result_push.returncode, "dvc push", result_push.stdout, result_push.stderr
                )
            print(f"DVC push success: {result_push.stdout}")
            
        except subprocess.CalledProcessError as e:
            # This is the expected failure point if AWS creds are wrong or missing
            error_message = f"DVC Error during {e.cmd}: {e.stderr}"
            print(f"CRITICAL DVC FAILURE: {error_message}")
            raise Exception(f"DVC failed. Check AWS credentials and S3 configuration: {error_message}")
        except Exception as e:
            # Other DVC related errors (e.g., DVC not installed)
            print(f"DVC versioning failed (DVC might not be installed or initialized): {e}")
            
        # --- MinIO Upload (Secondary Storage) ---
        try:
            from minio import Minio
            
            client = Minio(
                MINIO_ENDPOINT,
                access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                secure=False
            )
            
            if not client.bucket_exists(MINIO_BUCKET):
                client.make_bucket(MINIO_BUCKET)
            
            object_name = f"processed/processed_combined_{datetime.now().strftime('%Y%m%d')}.csv"
            client.fput_object(MINIO_BUCKET, object_name, DATA_PATH)
            print(f"Uploaded to MinIO: {MINIO_BUCKET}/{object_name}")
            
        except ImportError:
             print("MinIO library not installed. Skipping MinIO upload.")
        except Exception as e:
            print(f"MinIO upload skipped (connection or auth error): {e}")
        
        return {
            "status": "success",
            "rows_saved": len(df_new),
            "path": DATA_PATH
        }

    raw_data = extract_all_data()
    validated_data = strict_quality_gate(raw_data)
    transformed_data = transform_and_engineer_features(validated_data)
    profiled_data = generate_profiling_report(transformed_data)
    load_version_and_store(profiled_data)

dag = mandi_automation()