from PredictitData import get_all_contracts
from datetime import datetime
import MySQLdb as mdb
import re

def sql_friendly(text):
    """ Converts apostrophes to double underscores for MySQL friendliness. """
    try:
        return text.replace("'","__")
    except:
        return text

def insert_one_contract(contract_ticker, contract_data):
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

    print sql_statement

    con = mdb.connect('localhost', 'root', '', 'predictit_db');

    with con:

        cur = con.cursor()
        cur.execute(sql_statement)


def insert_all_contracts(contract_tickers):
    """ Inserts all contracts for a market_ticker into the table all_contracts. """

    for contract_ticker, contract_data in contract_tickers.iteritems():
        insert_one_contract(contract_ticker, contract_data)
