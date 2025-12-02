# Base image for Python applications
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# 1. Install dependencies
# Copy requirements.txt from the api/ directory relative to the build context (.)
COPY ../api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the trained model file. 
# The model file is at ./models/mandi_pipeline.pkl relative to the builad context.
# We create the destination directory first for clarity and safety.
RUN mkdir -p /app/models
COPY models/mandi_pipeline.pkl /app/models/mandi_pipeline.pkl

# 3. Copy the application code
COPY api/app.py /app/app.py

# Expose the port the application runs on
EXPOSE 8000

# Set the entry point to run the application using uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
