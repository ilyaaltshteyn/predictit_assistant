# This script matches each news headline to a contract. This will make it
# possible to include news headlines data in the prediction models.
import sqlalchemy as sqa
import pandas as pd
import re
from math import floor
import string
from collections import Counter

from nltk.corpus import stopwords
stops = stopwords.words('english')


def pre_process(text):
    """ Converts apostrophes to double underscores for MySQL friendliness. """

    # First do everything that is done to text that enters mysql:
    try:
        text = str(text)
    except (UnicodeEncodeError, UnicodeDecodeError):
        pass
    if not text or text == '':
        text = 'missing__data'.encode('ascii', 'ignore')
    try:
        text = text.replace("'", "__").replace(",", "__").replace('"', "__").replace("%", "__").encode('ascii', 'ignore')
    except:
        text = text.encode('ascii', 'ignore')

    # Strip punctuation:
    return text.translate(None, string.punctuation).lower()

def get_contract_metadata(contract_ticker):
    """ Pulls all data for contract, up to limit rows. """

    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    sql_statement = "select * from all_contracts_metadata where contract_ticker='{}' limit 1".format(contract_ticker)
    df = pd.read_sql(sql_statement, con)

    return df


def generate_new_stopwords_list(nltk_stopwords):
    """ Combines nltk stopwords list with the top 3 % of most common words in the contract descriptions. """
    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    metadata_for_all_contracts = pd.read_sql("select distinct(longname) from all_contracts_metadata",
                                             con)

    flatten = lambda l: [item for sublist in l for item in sublist]
    words = flatten([re.findall('\w+', x[0]) for x in metadata_for_all_contracts.values])
    preprocessed_words = [pre_process(word) for word in words]

    word_counts = Counter(cap_words)
    top_3_percent = int(floor(len(word_counts.items())*.03))
    common_words_w_counts = word_counts.most_common(top_3_percent)
    new_stop_words = [word for word, count in common_words_w_counts]
    stops = [w.encode('ascii', 'ignore') for w in nltk_stopwords + new_stop_words]

    return stops


def strip_stopwords(text, stopwords):
    """ Takes in some text (this will be the longname or a news headline) and a list of stopwords.
        Removes stopwords from text. Returns remaining words in text as list. """
    text = str(text).encode('ascii', 'ignore')
    split_preprocessed = [pre_process(word) for word in text.split()]
    return [w for w in split_preprocessed if w not in stopwords]

stops = generate_new_stopwords_list(stops)
strip_stopwords('Will Jim Webb be the Secretary of Defense on Feb. 28__ 2017?', stops)
