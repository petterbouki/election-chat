PDF_PATH ?= data/raw/edan_2025.pdf
DB_PATH  ?= data/elections.duckdb
DPI      ?= 250

.PHONY: help ingest run test eval clean lint

help:
	@echo ""
	@echo "  make ingest   — Pipeline OCR → DuckDB"
	@echo "  make run      — Lance l'app Streamlit"
	@echo "  make test     — Tests unitaires (pytest)"
	@echo "  make eval     — Suite d'évaluation offline"
	@echo "  make clean    — Supprime la base et les CSV"
	@echo "  make lint     — Vérification code (ruff)"
	@echo ""

ingest:
	python -m ingestion.pipeline --pdf $(PDF_PATH) --db $(DB_PATH) --dpi $(DPI)

run:
	streamlit run app/main.py

test:
	pytest tests/ -v --tb=short

eval:
	python evals/eval_runner.py --db $(DB_PATH)

clean:
	rm -f $(DB_PATH)
	rm -f data/raw/circonscriptions.csv data/raw/candidats.csv

lint:
	ruff check . --fix
