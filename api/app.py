"""
Phase IV: FastAPI with Prometheus Metrics
REST API for predictions with monitoring endpoints.
"""
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel
import joblib
import pandas as pd
import os
import time
from datetime import datetime

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

app = FastAPI(title="Mandi AI Price Predictor", version="2.0.0")

# ============================================================
# PROMETHEUS METRICS (Phase IV)
# ============================================================
REQUEST_COUNT = Counter(
    'mandi_api_requests_total', 
    'Total API requests',
    ['method', 'endpoint', 'status']
)

REQUEST_LATENCY = Histogram(
    'mandi_api_request_latency_seconds',
    'Request latency in seconds',
    ['endpoint'],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

PREDICTION_VALUE = Gauge(
    'mandi_last_prediction_value',
    'Last prediction value (price per 100kg)'
)

DATA_DRIFT_RATIO = Gauge(
    'mandi_data_drift_ratio',
    'Ratio of out-of-distribution requests'
)

# Track drift (simple implementation)
_total_requests = 0
_drift_requests = 0

# Known valid ranges (from training data)
VALID_RANGES = {
    'price_min': 50,
    'price_max': 50000,
    'known_cities': ['Lahore', 'Karachi', 'Islamabad', 'Faisalabad', 'Multan', 'Peshawar', 'Quetta'],
    'known_crops': ['Onion', 'Tomato', 'Potato', 'Rice', 'Wheat', 'Cotton', 'Sugarcane']
}

# ============================================================
# LOAD MODEL
# ============================================================
MODEL_PATH = os.getenv("MODEL_PATH", "models/mandi_pipeline.pkl")
if not os.path.exists(MODEL_PATH):
    MODEL_PATH = "/app/models/mandi_pipeline.pkl"

try:
    model = joblib.load(MODEL_PATH)
    print(f"Model loaded: {MODEL_PATH}")
except Exception as e:
    model = None
    print(f"Model load failed: {e}")

# ============================================================
# REQUEST/RESPONSE MODELS
# ============================================================
class PriceRequest(BaseModel):
    city: str
    crop: str
    date: str

class PriceResponse(BaseModel):
    query: dict
    predicted_price_100kg: float
    estimated_price_per_kg: float
    alert: str
    
class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    timestamp: str
    version: str

# ============================================================
# MIDDLEWARE FOR METRICS
# ============================================================
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    latency = time.time() - start_time
    
    # Record metrics
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()
    
    REQUEST_LATENCY.labels(endpoint=request.url.path).observe(latency)
    
    return response

# ============================================================
# ENDPOINTS
# ============================================================
@app.get("/")
def root():
    return {"message": "Mandi AI Price Predictor API", "version": "2.0.0"}

@app.get("/health", response_model=HealthResponse)
def health_check():
    """Health check endpoint."""
    return HealthResponse(
        status="healthy" if model else "degraded",
        model_loaded=model is not None,
        timestamp=datetime.now().isoformat(),
        version="2.0.0"
    )

@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.post("/predict", response_model=PriceResponse)
def predict(request: PriceRequest):
    """Make price prediction."""
    global _total_requests, _drift_requests
    
    if not model:
        raise HTTPException(status_code=500, detail="Model not loaded")
    
    try:
        _total_requests += 1
        
        # Check for data drift (unknown city/crop)
        is_drift = False
        if request.city not in VALID_RANGES['known_cities']:
            is_drift = True
        if request.crop not in VALID_RANGES['known_crops']:
            is_drift = True
            
        if is_drift:
            _drift_requests += 1
        
        # Update drift ratio metric
        drift_ratio = _drift_requests / _total_requests if _total_requests > 0 else 0
        DATA_DRIFT_RATIO.set(drift_ratio)
        
        # Parse date
        dt = pd.to_datetime(request.date)
        
        # Prepare input
        input_data = pd.DataFrame({
            'City': [request.city],
            'Crop': [request.crop],
            'Year': [dt.year],
            'Month': [dt.month],
            'Day': [dt.day]
        })
        
        # Predict
        predicted_price_100kg = float(model.predict(input_data)[0])
        price_per_kg = predicted_price_100kg / 100
        
        # Update prediction gauge
        PREDICTION_VALUE.set(predicted_price_100kg)
        
        # Determine alert level
        if price_per_kg > 300:
            alert = "High Volatility"
        elif is_drift:
            alert = "Unknown Input - Prediction may be unreliable"
        else:
            alert = "Stable"
        
        return PriceResponse(
            query=request.dict(),
            predicted_price_100kg=round(predicted_price_100kg, 2),
            estimated_price_per_kg=round(price_per_kg, 2),
            alert=alert
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/drift")
def get_drift_stats():
    """Get data drift statistics."""
    global _total_requests, _drift_requests
    return {
        "total_requests": _total_requests,
        "drift_requests": _drift_requests,
        "drift_ratio": _drift_requests / _total_requests if _total_requests > 0 else 0
    }
