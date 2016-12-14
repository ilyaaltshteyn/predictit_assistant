# Helper function for pulling data for a given contract_ticker

import sqlalchemy as sqa
import MySQLdb
import pandas as pd
import matplotlib
import numpy as np
import matplotlib.pyplot as plt


def get_contract_data(contract_ticker, limit = 100):
    """ Pulls all data for contract, up to limit rows. """

    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    sql_statement = "select * from all_contracts where contract_ticker='{}' limit {}".format(contract_ticker,
                                                                                             limit)
    df = pd.read_sql(sql_statement, con)

    return df

def plot_yes_shares(contract_ticker, limit = 100):
    """ Pulls data for a contract, up to limit rows. Plots this data.
        Must have enabled %matplotlib inline """

    dat = get_contract_data(contract_ticker, limit)
    dat = dat[['last_trade_price', 'best_buy_yes_cost', 'best_sell_yes_cost']]

    dat.plot.line(figsize = (13,13), title = contract_ticker, ylim = (0,1))
