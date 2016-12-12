import PredictitData as pred
import insert_contracts_data as ins


all_market_tickers = pred.get_all_market_ids()

for market_ticker in all_market_tickers[:5]:
    all_contracts = pred.get_all_contracts(market_ticker)
    ins.insert_all_contracts(all_contracts)
