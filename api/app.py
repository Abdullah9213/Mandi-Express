from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import pandas as pd
import os

app = FastAPI(title="Mandi AI Price Predictor")

# Load Model
MODEL_PATH = "models/mandi_pipeline.pkl"
if not os.path.exists(MODEL_PATH):
    MODEL_PATH = "/app/models/mandi_pipeline.pkl"

try:
    model = joblib.load(MODEL_PATH)
except Exception as e:
    model = None

class PriceRequest(BaseModel):
    city: str
    crop: str
    date: str

@app.post("/predict")
def predict(request: PriceRequest):
    if not model:
        raise HTTPException(status_code=500, detail="Model not loaded")
    
    try:
        dt = pd.to_datetime(request.date)
        input_data = pd.DataFrame({
            'City': [request.city],
            'Crop': [request.crop],
            'Year': [dt.year],
            'Month': [dt.month],
            'Day': [dt.day]
        })
        
        # Raw prediction is for 100 KG
        predicted_price_100kg = model.predict(input_data)[0]
        
        # Unit Conversion: 100kg -> 1kg
        price_per_kg = predicted_price_100kg / 100

        return {
            "query": request.dict(),
            "predicted_price_100kg": round(float(predicted_price_100kg), 2),
            "estimated_price_per_kg": round(float(price_per_kg), 2),
            "alert": "High Volatility" if price_per_kg > 300 else "Stable"
        }
        
    except Exception as e:
        return {"error": str(e)}
