# This script contains helper functions that pull all market IDs, all contract
# IDs for a given market ID, and price details for a given contract ID.

import requests


def get_all_market_ids():
    """ Pulls all data from Predictit, extracts and returns all market ids. """

    url = 'https://www.predictit.org/api/marketdata/all/'
    response_data = requests.get(url).json()

    return [market['TickerSymbol'] for market in response_data['Markets']]


def get_all_contracts(market_ticker):
    """ For a given market pulls all contract ids, their enddates, ticker symbols
        and statuses. """

    url = 'https://www.predictit.org/api/marketdata/ticker/{}'.format(market_ticker)
    response_data = requests.get(url).json()
    print response_data
    data = {}

    for contract in response_data['Contracts']:
        data[contract['TickerSymbol']] = {
                                          'market_ticker' : market_ticker,
                                          'end_date' : contract['DateEnd'],
                                          'status' : contract['Status'],
                                          'last_trade_price' : contract['LastTradePrice'],
                                          'best_buy_yes_cost' : contract['BestBuyYesCost'],
                                          'best_buy_no_cost' : contract['BestBuyNoCost'],
                                          'best_sell_yes_cost' : contract['BestSellYesCost'],
                                          'best_sell_no_cost' : contract['BestSellNoCost'],
                                          'last_close_price' : contract['LastClosePrice']
                                         }

    return data
