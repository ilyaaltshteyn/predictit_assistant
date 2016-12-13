# Helper function for pulling data for a given contract_ticker

import sqlalchemy as sqa
import MySQLdb
import pandas as pd

def get_contract_data(contract_ticker, limit = 100):
    """ Pulls all data for contract, up to limit rows. """

    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    sql_statement = "select * from all_contracts where contract_ticker='{}' limit {}".format(contract_ticker,
                                                                                             limit)
    df = pd.read_sql(sql_statement, con)

    return df
