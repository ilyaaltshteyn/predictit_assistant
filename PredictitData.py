# This script contains helper functions that pull all market IDs, all contract
# IDs for a given market ID, and price details for a given contract ID.

import requests


class DuplicateMarketId(Exception):
    pass


def get_all_market_ids():
    """ Pulls all data from Predictit, extracts and returns all market ids. """

    url = 'https://www.predictit.org/api/marketdata/all/'
    response_data = requests.get(url).json()

    data = {}

    for market in response_data['Markets']:
        if market['ID'] not in data:
            data[market['ID']] = market['TickerSymbol']
        else:
            raise DuplicateMarketId('Duplicate Market ID discovered...')

    return data


def get_all_contracts(market_id):
    """ For a given market pulls all contract ids, their enddates, ticker symbols
        and statuses. """

    url = 'https://www.predictit.org/api/marketdata/ticker/'
    
