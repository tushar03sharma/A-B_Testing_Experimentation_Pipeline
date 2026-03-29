PYTHON ?= python

install:
	$(PYTHON) -m pip install -e .[dev]

install-dashboard:
	$(PYTHON) -m pip install -e .[dashboard,dev]

generate:
	$(PYTHON) -m ab_testing_pipeline.cli generate

build:
	$(PYTHON) -m ab_testing_pipeline.cli build

analyze:
	$(PYTHON) -m ab_testing_pipeline.cli analyze

quality:
	$(PYTHON) -m ab_testing_pipeline.cli quality

dashboard:
	$(PYTHON) -m ab_testing_pipeline.cli dashboard

run:
	$(PYTHON) -m ab_testing_pipeline.cli run-all

test:
	pytest
