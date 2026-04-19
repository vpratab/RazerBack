PYTHON ?= python3
DATA_DIR ?= data
CONFIG ?= configs/continuation_portfolio_total_v1.json
OUTPUT_DIR ?=

.PHONY: install enrich run smoke

install:
	$(PYTHON) -m pip install -r requirements.txt

enrich:
	$(PYTHON) enrich_forex_research_data.py --data-dir $(DATA_DIR)

run:
	$(PYTHON) run_locked_portfolio.py --config $(CONFIG) --data-dir $(DATA_DIR) $(if $(OUTPUT_DIR),--output-dir $(OUTPUT_DIR),)

smoke:
	$(PYTHON) -m py_compile continuation_core.py locked_portfolio_runtime.py realistic_backtest.py fetch_oanda_bid_ask.py enrich_forex_research_data.py run_locked_portfolio.py
