#!/usr/bin/env python
# This script hits the predictit API for a list of all market tickers, then for
# each market ticker it gets all contracts within that market and loads them
# into the all_contracts table in the predictit_db schema in MySQL.

import PredictitData as pred

all_market_tickers = pred.get_all_market_ids()

for market_ticker in all_market_tickers:
    all_contracts = pred.get_all_contracts(market_ticker)
    pred.insert_all_contracts(all_contracts)
