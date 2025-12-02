# Use lightweight Python
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy model and code
# We copy the 'models' folder from the project root into the container
COPY ./models /app/models
COPY ./api/app.py /app/app.py

# Install dependencies
RUN pip install fastapi uvicorn pandas scikit-learn xgboost joblib

# Expose port
EXPOSE 8000

# Start command
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
