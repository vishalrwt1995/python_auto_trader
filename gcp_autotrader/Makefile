PYTHONPATH ?= src

.PHONY: install health bootstrap-sheets universe-sync premarket scan api test

install:
	python3 -m pip install -r requirements.txt

health:
	PYTHONPATH=$(PYTHONPATH) python3 -m autotrader.jobs health

bootstrap-sheets:
	PYTHONPATH=$(PYTHONPATH) python3 -m autotrader.jobs bootstrap-sheets

universe-sync:
	PYTHONPATH=$(PYTHONPATH) python3 -m autotrader.jobs universe-sync

premarket:
	PYTHONPATH=$(PYTHONPATH) python3 -m autotrader.jobs premarket-precompute

scan:
	PYTHONPATH=$(PYTHONPATH) python3 -m autotrader.jobs scan-once --force

api:
	PYTHONPATH=$(PYTHONPATH) uvicorn autotrader.main:app --reload --host 0.0.0.0 --port 8080

test:
	PYTHONPATH=$(PYTHONPATH) pytest -q

