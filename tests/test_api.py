"""
Phase III: Unit Tests for API
"""
import pytest
from fastapi.testclient import TestClient
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))

# Mock the model before importing app
import joblib
from unittest.mock import MagicMock, patch

# Create a mock model
mock_model = MagicMock()
mock_model.predict.return_value = [15000.0]  # Price for 100kg

with patch('joblib.load', return_value=mock_model):
    from app import app

client = TestClient(app)


class TestHealthEndpoint:
    """Test /health endpoint."""
    
    def test_health_returns_200(self):
        response = client.get("/health")
        assert response.status_code == 200
    
    def test_health_returns_status(self):
        response = client.get("/health")
        data = response.json()
        assert "status" in data
        assert "model_loaded" in data
        assert "timestamp" in data


class TestRootEndpoint:
    """Test / endpoint."""
    
    def test_root_returns_200(self):
        response = client.get("/")
        assert response.status_code == 200
    
    def test_root_returns_message(self):
        response = client.get("/")
        data = response.json()
        assert "message" in data
        assert "version" in data


class TestMetricsEndpoint:
    """Test /metrics endpoint."""
    
    def test_metrics_returns_200(self):
        response = client.get("/metrics")
        assert response.status_code == 200
    
    def test_metrics_returns_prometheus_format(self):
        response = client.get("/metrics")
        assert "mandi_api" in response.text or "python" in response.text


class TestPredictEndpoint:
    """Test /predict endpoint."""
    
    def test_predict_valid_request(self):
        response = client.post("/predict", json={
            "city": "Lahore",
            "crop": "Onion",
            "date": "2024-01-15"
        })
        assert response.status_code == 200
    
    def test_predict_returns_required_fields(self):
        response = client.post("/predict", json={
            "city": "Lahore",
            "crop": "Onion",
            "date": "2024-01-15"
        })
        data = response.json()
        assert "predicted_price_100kg" in data
        assert "estimated_price_per_kg" in data
        assert "alert" in data
    
    def test_predict_invalid_date(self):
        response = client.post("/predict", json={
            "city": "Lahore",
            "crop": "Onion",
            "date": "invalid-date"
        })
        assert response.status_code == 400


class TestDriftEndpoint:
    """Test /drift endpoint."""
    
    def test_drift_returns_200(self):
        response = client.get("/drift")
        assert response.status_code == 200
    
    def test_drift_returns_stats(self):
        response = client.get("/drift")
        data = response.json()
        assert "total_requests" in data
        assert "drift_requests" in data
        assert "drift_ratio" in data
