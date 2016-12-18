# This script matches each news headline to a contract. This will make it
# possible to include news headlines data in the prediction models.
import sqlalchemy as sqa
import pandas as pd
import re
from math import floor
from collections import Counter

from nltk.corpus import stopwords
stops = stopwords.words('english')

def get_contract_metadata(contract_ticker):
    """ Pulls all data for contract, up to limit rows. """

    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    sql_statement = "select * from all_contracts_metadata where contract_ticker='{}' limit 1".format(contract_ticker)
    df = pd.read_sql(sql_statement, con)

    return df

def generate_new_stopwords_list():
    """ Combines nltk stopwords list with the top 3 % of most common words in the contract descriptions. """
    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    metadata_for_all_contracts = pd.read_sql("select distinct(longname) from all_contracts_metadata",
                                             con)
    flatten = lambda l: [item for sublist in l for item in sublist]
    words = flatten([re.findall('\w+', x[0]) for x in metadata_for_all_contracts.values])
    cap_words = [word.upper() for word in words]
    word_counts = Counter(cap_words)
    top_3_percent = int(floor(len(word_counts.items())*.03))
    common_words_w_counts = word_counts.most_common(top_3_percent)
    new_stop_words = [word for word, count in common_words_w_counts]
    stops = [w.encode('ascii', 'ignore') for w in stops + new_stop_words]

    return stops
