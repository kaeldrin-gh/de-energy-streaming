COMPOSE ?= docker compose
TF_VARS = -var localstack_endpoint=http://localstack:4566

.PHONY: help up down ps logs init infra demo live stream backfill obs test lint fmt sample clean

help: ## show available targets
	@echo "de-energy-streaming"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-8s %s\n", $$1, $$2}'

up: ## start core stack (Airflow, Spark, Kafka, Postgres, LocalStack) and provision S3
	$(COMPOSE) up -d --build
	$(MAKE) infra

down: ## stop the stack (keeps volumes)
	$(COMPOSE) down

clean: ## stop the stack and delete all volumes (fresh start)
	$(COMPOSE) down -v

ps: ## show running services
	$(COMPOSE) ps

logs: ## tail logs
	$(COMPOSE) logs -f --tail=100

infra: ## terraform init + apply against LocalStack (creates the lakehouse bucket)
	$(COMPOSE) run --rm terraform init -input=false
	$(COMPOSE) run --rm terraform apply -auto-approve $(TF_VARS)

demo: ## replay the bundled real SMARD sample into Kafka (no network needed)
	$(COMPOSE) run --rm producer replay --speed 50

live: ## poll the live SMARD API and publish to Kafka (no key required)
	$(COMPOSE) run --rm producer live

stream: ## run the Kafka -> Iceberg streaming job in the foreground
	$(COMPOSE) run --rm spark-submit

backfill: ## revision-aware backfill from the live SMARD API (default: 4 weeks)
	$(COMPOSE) run --rm spark-submit --master spark://spark-master:7077 /opt/jobs/backfill_prices.py --weeks 4

obs: ## start Prometheus + Grafana (observability profile)
	$(COMPOSE) --profile observability up -d

test: ## run the unit tests locally (no Docker needed)
	python -m pytest -q

lint: ## ruff lint
	python -m ruff check .

fmt: ## ruff format
	python -m ruff format .

sample: ## refresh data/sample/ from the live SMARD API (needs network)
	python scripts/make_sample.py
