# Use lightweight Python
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy model and code
COPY ./models /app/models
COPY ./api/app.py /app/app.py

RUN pip install fastapi uvicorn pandas scikit-learn joblib prometheus-client

# Expose port
EXPOSE 8000

# Start command
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
