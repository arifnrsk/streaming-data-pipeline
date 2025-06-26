# Streaming Data Pipeline Makefile
# Compatible with Windows PowerShell and macOS/Linux

# Variables
PYTHON := python
VENV_DIR := venv
KAFKA_PORT := 9092
ZOOKEEPER_PORT := 2181

# Default target
.PHONY: help
help:
	@echo "Available commands:"
	@echo "  setup-env        - Create virtual environment and install dependencies"
	@echo "  install          - Install all dependencies"
	@echo "  quickstart       - Run health check and validation"
	@echo "  format           - Format code with black"
	@echo "  lint             - Run flake8 linting"
	@echo "  test             - Run pytest tests"
	@echo "  start-kafka      - Start Kafka and Zookeeper (requires Docker)"
	@echo "  stop-kafka       - Stop Kafka and Zookeeper"
	@echo "  run-producer     - Start the event producer"
	@echo "  run-consumer     - Start the streaming consumer"
	@echo "  run-pipeline     - Run complete pipeline (producer + consumer)"
	@echo "  clean            - Clean up temporary files"
	@echo "  clean-all        - Clean everything including venv"

# Environment setup
.PHONY: setup-env
setup-env:
ifeq ($(OS),Windows_NT)
	$(PYTHON) -m venv $(VENV_DIR)
	$(VENV_DIR)\Scripts\activate.bat
	$(VENV_DIR)\Scripts\pip install -r requirements.txt
else
	$(PYTHON) -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install -r requirements.txt
endif

.PHONY: install
install:
ifeq ($(OS),Windows_NT)
	$(VENV_DIR)\Scripts\pip install -r requirements.txt
else
	$(VENV_DIR)/bin/pip install -r requirements.txt
endif

# Code quality
.PHONY: format
format:
ifeq ($(OS),Windows_NT)
	$(VENV_DIR)\Scripts\black src\ tests\
else
	$(VENV_DIR)/bin/black src/ tests/
endif

.PHONY: lint
lint:
ifeq ($(OS),Windows_NT)
	$(VENV_DIR)\Scripts\flake8 src\ tests\
else
	$(VENV_DIR)/bin/flake8 src/ tests/
endif

.PHONY: test
test:
ifeq ($(OS),Windows_NT)
	$(VENV_DIR)\Scripts\pytest tests\ -v
else
	$(VENV_DIR)/bin/pytest tests/ -v
endif

# Kafka services
.PHONY: start-kafka
start-kafka:
	docker-compose up -d

.PHONY: stop-kafka
stop-kafka:
	docker-compose down

# Application runners
.PHONY: run-producer
run-producer:
ifeq ($(OS),Windows_NT)
	$(VENV_DIR)\Scripts\python -m src.producer.event_producer
else
	$(VENV_DIR)/bin/python -m src.producer.event_producer
endif

.PHONY: run-consumer
run-consumer:
ifeq ($(OS),Windows_NT)
	@echo "Setting HADOOP_HOME and running streaming consumer..."
	@powershell -Command "$$env:HADOOP_HOME='C:\hadoop'; $$env:PATH+=';C:\hadoop\bin'; venv\Scripts\python -m src.consumer.streaming_job"
else
	$(VENV_DIR)/bin/python -m src.consumer.streaming_job
endif

.PHONY: quickstart
quickstart:
ifeq ($(OS),Windows_NT)
	$(VENV_DIR)\Scripts\python scripts\quickstart.py
else
	$(VENV_DIR)/bin/python scripts/quickstart.py
endif

.PHONY: run-pipeline
run-pipeline:
	@echo "Starting complete pipeline..."
	@echo "1. Make sure Kafka is running: make start-kafka"
	@echo "2. Run producer in one terminal: make run-producer"  
	@echo "3. Run consumer in another terminal: make run-consumer"

# Cleanup
.PHONY: clean
clean:
ifeq ($(OS),Windows_NT)
	if exist __pycache__ rmdir /s /q __pycache__
	if exist .pytest_cache rmdir /s /q .pytest_cache
	if exist src\__pycache__ rmdir /s /q src\__pycache__
	for /d /r . %%d in (__pycache__) do @if exist "%%d" rmdir /s /q "%%d"
else
	find . -type d -name "__pycache__" -delete
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
endif

.PHONY: clean-all
clean-all: clean
ifeq ($(OS),Windows_NT)
	if exist $(VENV_DIR) rmdir /s /q $(VENV_DIR)
else
	rm -rf $(VENV_DIR)
endif 