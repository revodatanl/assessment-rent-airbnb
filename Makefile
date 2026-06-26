.PHONY: install test lint run clean pre-commit

install:
	pip install -e ".[dev]"

test:
	PYSPARK_PYTHON=$${PYSPARK_PYTHON:-python} PYSPARK_DRIVER_PYTHON=$${PYSPARK_DRIVER_PYTHON:-python} \
		pytest tests/ -v --tb=short

test-cov:
	pytest tests/ --cov=rent_airbnb --cov-report=term-missing

lint:
	ruff check src/ tests/
	ruff format --check src/ tests/

format:
	ruff format src/ tests/
	ruff check --fix src/ tests/

pre-commit:
	pre-commit install
	pre-commit run --all-files

run:
	PYSPARK_PYTHON=$${PYSPARK_PYTHON:-python} PYSPARK_DRIVER_PYTHON=$${PYSPARK_DRIVER_PYTHON:-python} \
		python -m rent_airbnb.pipeline

stream-prepare:
	python scripts/prepare_streaming_source.py --limit 50

stream:
	PYSPARK_PYTHON=$${PYSPARK_PYTHON:-python} PYSPARK_DRIVER_PYTHON=$${PYSPARK_DRIVER_PYTHON:-python} \
		python -c "from rent_airbnb.streaming import run_rentals_streaming_pipeline; \
		run_rentals_streaming_pipeline(terminate_after_ms=60000)"

clean:
	rm -rf data/output/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
