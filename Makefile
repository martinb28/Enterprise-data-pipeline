# Enterprise Data Pipeline — Developer Commands
# Usage: make <target>

.DEFAULT_GOAL := help
PROJECT_NAME  := enterprise-data-pipeline
DOCKER_DIR    := docker
DOCKER        := /usr/local/bin/docker
COMPOSE       := /usr/local/bin/docker compose -f $(DOCKER_DIR)/docker-compose.yml
PYTHON        := python3

.PHONY: help up down logs build clean test lint format pipeline-run \
        bronze silver gold ingest-faker ingest-kaggle minio-ui airflow-ui

# ─────────────────────────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────────────────────────
help: ## Show this help message
	@echo ""
	@echo "  🚀 Enterprise Data Pipeline — Developer Commands"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ─────────────────────────────────────────────────────────────────
# Infrastructure
# ─────────────────────────────────────────────────────────────────
up: ## Start all services (Spark + Airflow + MinIO + Jupyter)
	@echo "🚀 Starting Enterprise Data Pipeline stack..."
	@cp -n .env.example .env 2>/dev/null || true
	$(COMPOSE) up -d
	@echo ""
	@echo "✅ Stack is up! Access:"
	@echo "   Airflow UI  → http://localhost:8080  (admin/admin)"
	@echo "   MinIO UI    → http://localhost:9001  (minioadmin/minioadmin)"
	@echo "   Jupyter Lab → http://localhost:8888"
	@echo "   Spark UI    → http://localhost:8085"
	@echo ""

down: ## Stop all services
	@echo "⏹  Stopping services..."
	$(COMPOSE) down

restart: ## Restart all services
	$(COMPOSE) restart

build: ## Build Docker images (run after changing Dockerfiles)
	@echo "🔨 Building Docker images for ARM64 (Apple Silicon)..."
	$(COMPOSE) build --no-cache

logs: ## Tail logs for all services
	$(COMPOSE) logs -f

logs-spark: ## Tail Spark master logs
	$(COMPOSE) logs -f spark-master

logs-airflow: ## Tail Airflow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

status: ## Show container status
	$(COMPOSE) ps

# ─────────────────────────────────────────────────────────────────
# Pipeline Execution
# ─────────────────────────────────────────────────────────────────
ingest-faker: ## Run Bronze ingestion with Faker (no Kaggle needed)
	@echo "📦 Running Bronze ingestion with synthetic data..."
	$(COMPOSE) exec spark-master python -m src.ingestion.faker_generator

ingest-kaggle: ## Run Bronze ingestion with Kaggle API
	@echo "📦 Running Bronze ingestion from Kaggle..."
	$(COMPOSE) exec spark-master python -m src.ingestion.olist_loader

bronze: ## Run Bronze ingestion (auto-detects Kaggle or Faker)
	@echo "🥉 Running Bronze ingestion pipeline..."
	$(COMPOSE) exec airflow-webserver airflow dags trigger bronze_ingestion

silver: ## Run Silver transformation pipeline
	@echo "🥈 Running Silver transformation pipeline..."
	$(COMPOSE) exec airflow-webserver airflow dags trigger silver_processing

gold: ## Run Gold aggregation pipeline
	@echo "🥇 Running Gold aggregation pipeline..."
	$(COMPOSE) exec airflow-webserver airflow dags trigger gold_aggregation

pipeline-run: ## Run complete pipeline end-to-end (Bronze → Silver → Gold)
	@echo "🔄 Running complete pipeline Bronze → Silver → Gold..."
	@make ingest-faker
	@echo "⏳ Waiting for Bronze to complete..."
	@sleep 5
	$(COMPOSE) exec spark-master python -m src.transformations.bronze_to_silver
	$(COMPOSE) exec spark-master python -m src.transformations.silver_to_gold
	@echo "✅ Full pipeline complete!"

# ─────────────────────────────────────────────────────────────────
# Testing & Code Quality
# ─────────────────────────────────────────────────────────────────
test: ## Run all unit tests
	@echo "🧪 Running unit tests..."
	pytest tests/unit/ -v --tb=short --cov=src --cov-report=term-missing

test-coverage: ## Run tests with full HTML coverage report
	pytest tests/unit/ --cov=src --cov-report=html --cov-report=term-missing
	@echo "📊 Coverage report: open htmlcov/index.html"

lint: ## Run flake8 linter
	@echo "🔍 Linting..."
	flake8 src/ dags/ tests/ --max-line-length=100 --extend-ignore=E203,W503

format: ## Auto-format code with black and isort
	@echo "✨ Formatting code..."
	black src/ dags/ tests/
	isort src/ dags/ tests/

format-check: ## Check formatting without modifying files
	black --check src/ dags/ tests/
	isort --check-only src/ dags/ tests/

# ─────────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────────
minio-ui: ## Open MinIO console in browser
	open http://localhost:9001

airflow-ui: ## Open Airflow UI in browser
	open http://localhost:8080

jupyter: ## Open Jupyter Lab in browser
	open http://localhost:8888

spark-ui: ## Open Spark Master UI in browser
	open http://localhost:8085

setup-env: ## Copy .env.example to .env (first-time setup)
	@cp -n .env.example .env
	@echo "✅ .env file created — fill in your Kaggle credentials"
	@echo "   Edit .env and set KAGGLE_USERNAME and KAGGLE_KEY"

clean-data: ## Remove all data from MinIO buckets (start fresh)
	@echo "⚠️  This will delete all pipeline data in MinIO!"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ]
	$(COMPOSE) exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
	$(COMPOSE) exec minio mc rm --recursive --force local/bronze
	$(COMPOSE) exec minio mc rm --recursive --force local/silver
	$(COMPOSE) exec minio mc rm --recursive --force local/gold
	@echo "🗑️  MinIO data cleared"

clean: ## Remove all Docker containers, volumes, and local artifacts
	$(COMPOSE) down -v
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name htmlcov -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "🧹 Cleaned up"
