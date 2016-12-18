# This script contains helper functions that pull data from predictit and insert
# it into mysql.

import requests
from datetime import datetime
import MySQLdb as mdb
import re


def get_all_market_ids():
    """ Pulls all data from Predictit, extracts and returns all market ids. """

    url = 'https://www.predictit.org/api/marketdata/all/'
    response_data = requests.get(url).json()

    return [market['TickerSymbol'] for market in response_data['Markets']]


def get_all_contracts(market_ticker):
    """ For a given market pulls all contract data and returns it as a
        (price data dictionary, metadata dictionary) tuple. """

    url = 'https://www.predictit.org/api/marketdata/ticker/{}'.format(market_ticker)
    response_data = requests.get(url).json()
    data = {}
    metadata = {}

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
        metadata[contract['TickerSymbol']] = {
                                              'market_ticker' : market_ticker,
                                              'predictit_id' : contract['ID'],
                                              'url' : contract['URL'],
                                              'longname' : contract['LongName'],
                                              'shortname' : contract['ShortName']
                                              }


    return data, metadata


def sql_friendly(text):
    """ Converts apostrophes to double underscores for MySQL friendliness. """
    try:
        text = str(text)
    except (UnicodeEncodeError, UnicodeDecodeError):
        continue
    if not text or text == '':
        return 'missing__data'.encode('ascii', 'ignore')
    try:
        return text.replace("'", "__").replace(",", "__").replace('"', "__").replace("%", "__").encode('ascii', 'ignore')
    except:
        return text.encode('ascii', 'ignore')


def insert_one_contract(contract_ticker, contract_data, contract_metadata):
    """Inserts a single row into the all_contracts table, with details about a single contract
       at a single timepoint. """

    sql_statement = """INSERT INTO all_contracts VALUES ('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')""".format(
                                  str(datetime.now()),
                                  str(sql_friendly(contract_ticker)),
                                  sql_friendly(contract_data['market_ticker']),
                                  sql_friendly(contract_data['end_date']),
                                  sql_friendly(contract_data['status']),
                                  sql_friendly(contract_data['last_trade_price']),
                                  sql_friendly(contract_data['best_buy_yes_cost']),
                                  sql_friendly(contract_data['best_buy_no_cost']),
                                  sql_friendly(contract_data['best_sell_yes_cost']),
                                  sql_friendly(contract_data['best_sell_no_cost']),
                                  sql_friendly(contract_data['last_close_price'])
                                 )

    metadata_sql_statement = """REPLACE INTO all_contracts_metadata
                                SET contract_ticker = '{0}', market_ticker = '{1}',
                                predictit_id = {2}, url = '{3}', longname = '{4}',
                                shortname = '{5}'""".format(sql_friendly(contract_ticker),
                                                            sql_friendly(contract_metadata['market_ticker']),
                                                            contract_metadata['predictit_id'],
                                                            sql_friendly(contract_metadata['url']),
                                                            sql_friendly(contract_metadata['longname']),
                                                            sql_friendly(contract_metadata['shortname'])
                                                            )

    con = mdb.connect('localhost', 'root', '', 'predictit_db');

    with con:

        cur = con.cursor()
        cur.execute(sql_statement)
        cur.execute(metadata_sql_statement)


def insert_all_contracts(contract_tickers):
    """ Inserts all contracts for a market_ticker into the table all_contracts. """

    for contract_ticker, contract_data in contract_tickers[0].iteritems():
        insert_one_contract(contract_ticker, contract_tickers[0][contract_ticker], contract_tickers[1][contract_ticker])
