from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import os
import re
import json
import requests
import socket
import subprocess # Necessary for executing MinIO/DVC commands if not using dedicated operators
import mlflow # Moved import to top level

# CONFIGURATION
DATA_PATH = "/usr/local/airflow/include/processed_combined.csv"
RAW_DATA_DIR = "/usr/local/airflow/include/raw_history"
URL = "http://www.amis.pk/daily%20market%20changes.aspx"

# MLOps Configuration (Must be set in Airflow/Env Vars)
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000") # Default to local
DAGSHUB_USERNAME = os.getenv("DAGSHUB_USERNAME")
DAGSHUB_TOKEN = os.getenv("DAGSHUB_TOKEN")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "mandi-data")


default_args = {
    'owner': 'airflow',
    # Set a robust retry count and delay
    'retries': 3, 
    'retry_delay': timedelta(minutes=2),
}

# --- HELPER FUNCTIONS (Moved outside DAG for clarity and testing) ---

def clean_crop_name(name):
    """Removes Urdu characters and extra parentheses."""
    if not isinstance(name, str):
        return str(name)
    # This regex is better for removing content inside parentheses, and the Urdu clean
    clean_name = re.sub(r'\([^)]*\)', '', name)
    clean_name = re.sub(r'[^\x00-\x7F]+', '', clean_name)
    clean_name = re.sub(r'\s{2,}', ' ', clean_name).strip()
    return clean_name

def check_network_connectivity(url_to_check="8.8.8.8"):
    """Check if we can reach external sites via a quick DNS query."""
    try:
        socket.setdefaulttimeout(5)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((url_to_check, 53))
        return True
    except socket.error:
        return False

# --- DAG DEFINITION ---

@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1), 
    catchup=False, 
    default_args=default_args,
    tags=['mandi_ai', 'production', 'mlops'],
    description="V3: Resilient Data Ingestion with Strict Quality Gates and MinIO/MLflow Logging."
)
def mandi_automation_v3():

    # --- TASK 1: EXTRACTION WITH FALLBACK ---
    @task(retries=5) # Allow more retries for external network requests
    def extract_and_save_raw():
        """Extract data from AMIS, handle network failure gracefully, and save raw batch."""
        
        extraction_time = datetime.now()
        print(f"Attempting to connect to {URL}...")
        
        # 1. Check Connectivity
        if not check_network_connectivity():
            print("WARNING: No external network connectivity detected. Checking local data.")
            if os.path.exists(DATA_PATH):
                existing_df = pd.read_csv(DATA_PATH)
                last_date = existing_df['Date'].max()
                print(f"Falling back: Using data from last run ({last_date}) and re-stamping it for today.")
                
                # Use the last successful data batch and re-stamp it for today's run
                latest_data = existing_df[existing_df['Date'] == last_date].copy()
                latest_data['Date'] = extraction_time.strftime("%Y-%m-%d")
                
                return {
                    "data": latest_data.to_dict(orient='records'),
                    "raw_file": None,
                    "extraction_time": extraction_time.isoformat(),
                    "fallback": True
                }
            else:
                raise ConnectionError("CRITICAL FAILURE: No network and no local data available for fallback.")
        
        # 2. Perform Extraction (Internet Active)
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Airflow Mandi-AI pipeline)'}
            response = requests.get(URL, headers=headers, timeout=30)
            response.raise_for_status()
            
            from io import StringIO
            dfs = pd.read_html(StringIO(response.text))
            
            target_df = None
            for df in dfs:
                df.columns = [str(c).strip() for c in df.columns]
                # Look for key columns to identify the correct table
                if 'CityName' in df.columns and 'CropName' in df.columns:
                    target_df = df
                    break
            
            if target_df is None:
                # Fallback to the largest table if specific columns weren't found
                target_df = max(dfs, key=len)
                if target_df.shape[1] >= 3:
                    target_df.columns = ['CityName', 'CropName', "Today's FQP/Average Price"] + list(target_df.columns[3:])
                else:
                    raise ValueError("No viable data tables found on page.")

            # Standardize Columns and Clean
            target_df.rename(columns={
                'CityName': 'City', 
                'CropName': 'Crop', 
                "Today's FQP/Average Price": 'Price'
            }, inplace=True)
            
            # Clean and prepare data
            target_df['Price'] = target_df['Price'].astype(str).str.replace(',', '', regex=False)
            target_df['Price'] = pd.to_numeric(target_df['Price'], errors='coerce')
            target_df['Crop'] = target_df['Crop'].apply(clean_crop_name)
            target_df.dropna(subset=['City', 'Crop', 'Price'], inplace=True)
            
            final_df = target_df[['City', 'Crop', 'Price']].copy()
            final_df['Date'] = extraction_time.strftime("%Y-%m-%d")
            final_df['ExtractionTimestamp'] = extraction_time.isoformat()
            
            # Save Raw Data Locally
            os.makedirs(RAW_DATA_DIR, exist_ok=True)
            raw_filename = f"raw_{extraction_time.strftime('%Y%m%d_%H%M%S')}.csv"
            raw_path = os.path.join(RAW_DATA_DIR, raw_filename)
            final_df.to_csv(raw_path, index=False, encoding='utf-8-sig')
            print(f"Raw data saved to {raw_path}")

            print(f"✅ SUCCESS: Extracted {len(final_df)} rows.")
            return {
                "data": final_df.to_dict(orient='records'),
                "raw_file": raw_path,
                "extraction_time": extraction_time.isoformat(),
                "fallback": False
            }

        except Exception as e:
            print(f"❌ Critical Scrape/Process Error: {e}")
            # If the scrape fails, we raise an exception to let Airflow retry/fail the DAG
            raise

    # --- TASK 2: STRICT DATA QUALITY GATE (Fail-Fast) ---
    @task()
    def strict_quality_gate(extraction_result: dict):
        """Mandatory Quality Gate: checks schema, nulls, and price validity."""
        data_list = extraction_result.get("data", [])
        
        if not data_list:
            raise ValueError("QUALITY GATE FAILED: No data extracted.")
        
        df = pd.DataFrame(data_list)
        total_rows = len(df)
        
        required_columns = ['Date', 'City', 'Crop', 'Price']
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"QUALITY GATE FAILED: Missing columns: {missing_cols}")
        
        # Check Nulls
        for col in required_columns:
            null_pct = (df[col].isnull().sum() / total_rows) * 100
            if null_pct > 1.0:
                raise ValueError(
                    f"QUALITY GATE FAILED: Column '{col}' has {null_pct:.2f}% null values (threshold: 1%)"
                )
                
        # Check Price Validity
        if not pd.api.types.is_numeric_dtype(df['Price']):
            raise ValueError("QUALITY GATE FAILED: 'Price' column is not numeric")
            
        invalid_prices = df[df['Price'] <= 0]
        if len(invalid_prices) > 0 and (len(invalid_prices) / total_rows) * 100 > 5.0:
            raise ValueError(f"QUALITY GATE FAILED: {len(invalid_prices) / total_rows * 100:.2f}% of prices are invalid (<=0)")
        
        # Final clean: only use valid prices
        valid_df = df[df['Price'] > 0].dropna(subset=required_columns)
        
        print(f"✅ QUALITY GATE PASSED: {len(valid_df)}/{total_rows} records valid.")
        
        extraction_result['data'] = valid_df.to_dict(orient='records')
        extraction_result['valid_count'] = len(valid_df)
        extraction_result['total_count'] = total_rows
        return extraction_result

    # --- TASK 3: FEATURE ENGINEERING ---
    @task()
    def transform_and_engineer_features(validated_result: dict):
        """Adds time-based features, lag features, and rolling means for ML preparation."""
        df = pd.DataFrame(validated_result.get("data", []))
        
        # 1. Time Features
        df['Date'] = pd.to_datetime(df['Date'])
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month
        df['Day'] = df['Date'].dt.day
        df['DayOfWeek'] = df['Date'].dt.dayofweek
        df['WeekOfYear'] = df['Date'].dt.isocalendar().week.astype(int)
        
        # 2. Lag and Rolling Features (Requires historical data)
        if os.path.exists(DATA_PATH):
            historical_df = pd.read_csv(DATA_PATH)
            historical_df['Date'] = pd.to_datetime(historical_df['Date'])
            
            # Use only necessary columns from history
            historical_df = historical_df[['City', 'Crop', 'Price', 'Date']]
            
            combined = pd.concat([historical_df, df], ignore_index=True)
            combined = combined.sort_values(['City', 'Crop', 'Date']).drop_duplicates(
                subset=['City', 'Crop', 'Date'], keep='last'
            )
            
            # Calculate lag and rolling features on the combined data
            combined['Price_Lag_1'] = combined.groupby(['City', 'Crop'])['Price'].shift(1)
            combined['Price_Rolling_7d'] = combined.groupby(['City', 'Crop'])['Price'].transform(
                lambda x: x.rolling(window=7, min_periods=1).mean()
            )
            
            # Filter back to only today's data (now enriched)
            df = combined[combined['Date'].isin(df['Date'].unique())].copy()
            
        else:
            # If no history exists, initialize features
            df['Price_Lag_1'] = None
            df['Price_Rolling_7d'] = df['Price']
            
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
        
        print(f"Feature engineering complete. {len(df.columns)} columns now available.")
        
        validated_result['data'] = df.to_dict(orient='records')
        validated_result['features_added'] = ['Year', 'Month', 'Day', 'DayOfWeek', 'WeekOfYear', 'Price_Lag_1', 'Price_Rolling_7d']
        return validated_result

    # --- TASK 4: PERSIST AND STORE (Local CSV + MinIO/S3) ---
    @task()
    def persist_and_store_data(transformed_result: dict):
        """Append processed data to local CSV and push latest to MinIO/S3."""
        df_new = pd.DataFrame(transformed_result.get("data", []))
        
        # Ensure the final DataFrame is complete before saving
        final_cols = ['City', 'Date', 'Crop', 'Price', 'Year', 'Month', 'Day', 'DayOfWeek', 
                      'WeekOfYear', 'Price_Lag_1', 'Price_Rolling_7d']
        df_new = df_new.reindex(columns=final_cols)
        
        os.makedirs(os.path.dirname(DATA_PATH), exist_ok=True)
        today_str = df_new['Date'].iloc[0] if len(df_new) > 0 else None
        
        # 1. Local Append (Deduplication check)
        if os.path.exists(DATA_PATH) and today_str:
            existing_df = pd.read_csv(DATA_PATH, usecols=['Date'], parse_dates=['Date'])
            existing_dates = existing_df['Date'].dt.strftime('%Y-%m-%d').unique()
            
            if today_str in existing_dates:
                print(f"Data for {today_str} already exists in {DATA_PATH}. Skipping local append.")
                return {"status": "skipped", "rows_saved": 0}
            
            df_new.to_csv(DATA_PATH, mode='a', header=False, index=False, encoding='utf-8-sig')
            print(f"✅ Appended {len(df_new)} new rows to {DATA_PATH}.")
        else:
            df_new.to_csv(DATA_PATH, mode='w', header=True, index=False, encoding='utf-8-sig')
            print(f"✅ Created new data file at {DATA_PATH} with {len(df_new)} rows.")
            
        # 2. MinIO/S3 Upload (Recommended for persistent storage)
        try:
            from minio import Minio
            # In a real Airflow deployment, you would use an Airflow connection here
            client = Minio(
                MINIO_ENDPOINT,
                access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                secure=False # Use True for production S3/MinIO with HTTPS
            )
            
            bucket_name = MINIO_BUCKET
            if not client.bucket_exists(bucket_name):
                client.make_bucket(bucket_name)
            
            # Upload a timestamped snapshot of the combined file
            object_name = f"processed/full_data_snapshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            client.fput_object(bucket_name, object_name, DATA_PATH)
            print(f"✅ Uploaded combined snapshot to MinIO: {bucket_name}/{object_name}")
            
        except ImportError:
            print("⚠️ MinIO library not installed. Skipping remote storage.")
        except Exception as e:
            print(f"❌ MinIO upload failed (Check credentials/endpoint): {e}")

        # Note: DVC commands (subprocess.run) were removed as they are non-standard/fragile in Airflow.
        # Use a dedicated DVC/S3 operator or separate MLOps step if needed.
        
        return {
            "status": "saved",
            "rows_saved": len(df_new),
            "path": DATA_PATH
        }

    # --- TASK 5: GENERATE PROFILING REPORT ---
    @task()
    def generate_profiling_report(load_result: dict):
        """Generate data profiling report and compute basic metrics."""
        print("📊 Generating data profiling report...")
        
        if load_result.get("status") == "skipped":
             print("Data load skipped (duplicate date). Profiling skipped.")
             return {"status": "skipped"}

        if not os.path.exists(DATA_PATH):
            raise FileNotFoundError(f"Data file not found at {DATA_PATH}")
        
        df = pd.read_csv(DATA_PATH)
        
        # Calculate basic stats
        stats = {
            "total_rows": len(df),
            "unique_cities": int(df['City'].nunique()),
            "unique_crops": int(df['Crop'].nunique()),
            "avg_price": float(df['Price'].mean()) if 'Price' in df.columns else 0,
            "max_price": float(df['Price'].max()) if 'Price' in df.columns else 0,
            "null_count": int(df.isnull().sum().sum()),
        }
        
        # Try ydata-profiling (optional dependency)
        report_path = "/usr/local/airflow/include/reports"
        os.makedirs(report_path, exist_ok=True)
        report_file = os.path.join(report_path, f"profile_{datetime.now().strftime('%Y%m%d')}.html")

        try:
            from ydata_profiling import ProfileReport
            profile = ProfileReport(df, title="Mandi Data Quality Report", minimal=True)
            profile.to_file(report_file)
            print(f"✅ Profiling report saved: {report_file}")
            stats["profiling_report"] = report_file
        except ImportError:
            print("⚠️ ydata-profiling not installed. Generating basic stats only.")
        except Exception as e:
            print(f"⚠️ Profiling failed: {e}. Using basic stats only.")
            
        return stats
        
    # --- TASK 6: LOG TO MLFLOW ---
    @task()
    def log_mlflow_run(stats: dict):
        """Log metrics and artifacts to MLflow/DagsHub."""
        
        if stats.get("status") == "skipped":
            print("Skipping MLflow logging.")
            return {"status": "skipped"}
        
        if not MLFLOW_TRACKING_URI or not DAGSHUB_TOKEN or not DAGSHUB_USERNAME:
            print("⚠️ MLflow not configured. Skipping logging.")
            print("Set MLFLOW_TRACKING_URI, DAGSHUB_USERNAME, DAGSHUB_TOKEN.")
            return {"status": "skipped", "reason": "not_configured"}
        
        try:
            # Set authentication for DagsHub
            os.environ["MLFLOW_TRACKING_USERNAME"] = DAGSHUB_USERNAME
            os.environ["MLFLOW_TRACKING_PASSWORD"] = DAGSHUB_TOKEN
            
            mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
            mlflow.set_experiment("mandi_data_ingestion_v3")
            
            with mlflow.start_run(run_name=f"ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
                
                # Log run parameters
                mlflow.log_param("source_url", URL)
                mlflow.log_param("run_date", datetime.now().strftime("%Y-%m-%d"))
                
                # Log metrics
                for key, value in stats.items():
                    if isinstance(value, (int, float)):
                        mlflow.log_metric(key, value)
                
                # Log profiling report as an artifact
                report_file = stats.get("profiling_report")
                if report_file and os.path.exists(report_file):
                    mlflow.log_artifact(report_file, artifact_path="data_profiles")
                    print(f"✅ Profiling report logged to MLflow")
                
            print("✅ MLflow logging complete!")
            return {"status": "success", "tracking_uri": MLFLOW_TRACKING_URI}
            
        except Exception as e:
            print(f"❌ MLflow logging failed: {e}")
            return {"status": "error", "error": str(e)}

    # --- DAG FLOW DEFINITION ---
    raw_data = extract_and_save_raw()
    validated_data = strict_quality_gate(raw_data)
    transformed_data = transform_and_engineer_features(validated_data)
    load_result = persist_and_store_data(transformed_data)
    
    # Profiling and MLflow logging depend on the data being successfully loaded
    stats = generate_profiling_report(load_result)
    log_mlflow_run(stats)

dag = mandi_automation_v3()