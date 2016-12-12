# This script contains helper functions that pull all market IDs, all contract
# IDs for a given market ID, and price details for a given contract ID.

import requests


class DuplicateId(Exception):
    pass

def get_all_market_ids():
    """ Pulls all data from Predictit, extracts and returns all market ids. """

    url = 'https://www.predictit.org/api/marketdata/all/'
    response_data = requests.get(url).json()

    data = {}

    for market in response_data['Markets']:

        if market['ID'] not in data:
            data[market['ID']] = market['TickerSymbol']

        else: raise DuplicateId('Duplicate Market ID discovered...')

    return data


def get_all_contracts(market_ticker):
    """ For a given market pulls all contract ids, their enddates, ticker symbols
        and statuses. """

    url = 'https://www.predictit.org/api/marketdata/ticker/{}'.format(market_ticker)
    response_data = requests.get(url).json()

    data = {}

    for contract in response_data['Contracts']:

        if contract['ID'] not in data:
            data[contract['ID']] = {
                                    'end_date' : contract['DateEnd'],
                                    'ticker' : contract['TickerSymbol'],
                                    'status' : contract['Status']
                                    }

        else: raise DuplicateId('Duplicate Contract ID discovered...')

    return data


def get_price_data(contract_ticker):
    """ For a given contract pulls LastTradePrice, BestBuyYesCost, BestBuyNoCost,
        BestSellYesCost, BestSellNoCost, LastClosePrice. """

    url = 'https://www.predictit.org/api/marketdata/ticker/{}'.format(contract_ticker)
    response_data = requests.get(url).json()

    return {
            'last_trade_price' : response_data['LastTradePrice'],
            'best_buy_yes_cost' : response_data['BestBuyYesCost'],
            'best_buy_no_cost' : response_data['BestBuyNoCost'],
            'best_sell_yes_cost' : response_data['BestSellYesCost'],
            'best_sell_no_cost' : response_data['BestSellNoCost'],
            'last_close_price' : response_data['LastClosePrice']
            }
