PYTHON ?= python

install:
	$(PYTHON) -m pip install -e .[dev]

generate:
	$(PYTHON) -m ab_testing_pipeline.cli generate

build:
	$(PYTHON) -m ab_testing_pipeline.cli build

analyze:
	$(PYTHON) -m ab_testing_pipeline.cli analyze

quality:
	$(PYTHON) -m ab_testing_pipeline.cli quality

run:
	$(PYTHON) -m ab_testing_pipeline.cli run-all

test:
	pytest
