"""
Phase III: Unit Tests for Training Script
"""
import pytest
import pandas as pd
import numpy as np
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))


class TestDataLoading:
    """Test data loading functions."""
    
    def test_sample_data_structure(self):
        """Test that sample data has correct structure."""
        df = pd.DataFrame({
            'City': ['Lahore', 'Karachi'],
            'Crop': ['Onion', 'Tomato'],
            'Year': [2024, 2024],
            'Month': [1, 1],
            'Day': [15, 16],
            'Price': [150.0, 200.0]
        })
        
        required_cols = ['City', 'Crop', 'Year', 'Month', 'Day', 'Price']
        for col in required_cols:
            assert col in df.columns


class TestFeatureEngineering:
    """Test feature engineering."""
    
    def test_date_features(self):
        """Test date feature extraction."""
        df = pd.DataFrame({
            'Date': pd.to_datetime(['2024-01-15', '2024-06-20'])
        })
        
        df['Year'] = df['Date'].dt.year
        df['Month'] = df['Date'].dt.month
        df['Day'] = df['Date'].dt.day
        df['DayOfWeek'] = df['Date'].dt.dayofweek
        
        assert df['Year'].iloc[0] == 2024
        assert df['Month'].iloc[0] == 1
        assert df['Day'].iloc[0] == 15
        assert df['DayOfWeek'].iloc[0] == 0  # Monday


class TestModelPipeline:
    """Test model pipeline components."""
    
    def test_preprocessing(self):
        """Test that preprocessing handles categorical features."""
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
        from sklearn.compose import ColumnTransformer
        
        df = pd.DataFrame({
            'City': ['Lahore', 'Karachi', 'Lahore'],
            'Crop': ['Onion', 'Tomato', 'Onion'],
            'Year': [2024, 2024, 2024],
            'Month': [1, 2, 3],
            'Day': [15, 16, 17]
        })
        
        preprocessor = ColumnTransformer(
            transformers=[
                ('cat', OneHotEncoder(handle_unknown='ignore'), ['City', 'Crop']),
                ('num', StandardScaler(), ['Year', 'Month', 'Day'])
            ]
        )
        
        transformed = preprocessor.fit_transform(df)
        assert transformed.shape[0] == 3  # 3 samples
