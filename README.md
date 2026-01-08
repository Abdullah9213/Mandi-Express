# Mandi: Agricultural Price Prediction MLOps Pipeline

Mandi is a production-grade MLOps system designed to forecast agricultural commodity prices in Pakistan. It ingests market data from the Agriculture Marketing Information Service (AMIS) and implements a full Continuous Training (CT) and Continuous Deployment (CD) workflow to deliver reproducible model training, versioning, monitoring, and serving.

<img width="899" height="558" alt="Gemini_Generated_Image_gyn079gyn079gyn0" src="https://github.com/user-attachments/assets/f1dda777-4aeb-4df1-864c-929e8826618f" />


## System Architecture

The project follows a modular microservices architecture for scalability and reproducibility:

- Data Ingestion: Apache Airflow (Astro) runs automated ETL pipelines to extract and validate daily market data.
- Versioning: DVC (with an S3-compatible remote like MinIO or DagsHub) version-controls raw and processed datasets and model artifacts.
- Experiment Tracking: MLflow logs experiments, metrics (RMSE, MAE), parameters, and manages the Model Registry.
- CI/CD: GitHub Actions runs linting, tests, and CML reporting on pull requests to track model changes.
- Serving: The champion model is containerized and exposed via a FastAPI-based REST inference service.
- Observability: Prometheus and Grafana provide metrics, dashboards, and alerts for latency, system health, and data drift.

## Technology Stack

- Orchestration: Apache Airflow (Astro)
- Data Versioning: DVC + MinIO / DagsHub
- Experiment Tracking: MLflow
- Serving: FastAPI + Uvicorn
- CI/CD: GitHub Actions + CML
- Monitoring: Prometheus + Grafana
- Infrastructure: Docker Compose

## Project Structure

<img width="576" height="347" alt="{5659B704-1055-4B85-8DAE-6AD571CC5F77}" src="https://github.com/user-attachments/assets/e3f5da1c-c214-4337-9f21-88280052fbfd" />

## Quick Start

Prerequisites:

- Docker Desktop & Docker Compose
- Python 3.9+
- Astro CLI (optional, for Airflow dev environment)

1) Clone and configure

```bash
git clone https://dagshub.com/i222515/mandi.git
cd mandi
cp .env.example .env
# Edit .env to add your DagsHub/MLflow/MinIO credentials
```

2) Data synchronization with DVC

```bash
python -m dvc init  # only if DVC not already initialized locally
python -m dvc remote add -d dagshub https://dagshub.com/i222515/mandi.dvc
python -m dvc remote modify dagshub --local auth basic
python -m dvc remote modify dagshub --local user YOUR_USERNAME
python -m dvc remote modify dagshub --local password YOUR_TOKEN
python -m dvc pull
```

3) Start services

```bash
# Start monitoring, API, and storage
docker-compose up -d

# Start Airflow (if using Astro dev environment)
astro dev start
```

4) Access points (defaults)

- Prediction API: http://localhost:8000
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3001  (admin / admin)
- MinIO Console: http://localhost:9001  (minioadmin / minioadmin)
- Airflow UI: http://localhost:8080  (admin / admin)

## API Usage

Interactive docs: http://localhost:8000/docs

Example prediction request:

```bash
curl -X POST http://localhost:8000/predict \
	-H "Content-Type: application/json" \
	-d '{"city":"Lahore","crop":"Onion","date":"2024-01-15"}'
```

## Development Workflow

We follow GitFlow-style branching:

- `feature/*`: New features and experiments. Run linting locally.
- `dev`: Integration and CML-driven model comparison on PRs.
- `test`: Deployment verification.
- `master`: Production; CI builds and publishes Docker images.

Required secrets for CI (GitHub repo settings):

- DAGSHUB_USERNAME, DAGSHUB_TOKEN
- MLFLOW_TRACKING_URI
- DOCKER_USERNAME, DOCKER_PASSWORD

## Monitoring & Alerts

Prometheus alerting is configured for key conditions such as high latency and data drift. Example rules:

- High Latency: p95 > 500ms (warning)
- Data Drift: drift ratio > 30% (critical)

## License

This project is licensed under the MIT License.

---

