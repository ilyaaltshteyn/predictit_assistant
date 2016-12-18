# This script matches each news headline to a contract. This will make it
# possible to include news headlines data in the prediction models.
import sqlalchemy as sqa
import pandas as pd

def get_contract_metadata(contract_ticker):
    """ Pulls all data for contract, up to limit rows. """

    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    sql_statement = "select * from all_contracts_metadata where contract_ticker='{}' limit 1".format(contract_ticker)
    df = pd.read_sql(sql_statement, con)

    return df
