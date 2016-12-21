import sqlalchemy as sqa
import pandas as pd
import re
from datetime import datetime, timedelta
from math import floor
import string
from collections import Counter

from nltk.corpus import stopwords


class headline_finder():

    def __init__(self):
        stops = stopwords.words('english')
        self.stopwords = self.generate_new_stopwords_list(stops)
        self.last_contract_description = 'there was no last contract...'


    def generate_new_stopwords_list(self, nltk_stopwords):
        """ Combines nltk stopwords list with the top 3 % of most common words in
            the contract descriptions. Also add numbers 0-100 and 2000-2020. """

        nums = [str(x) for x in range(0, 101)] + [str(x) for x in range(2000, 2021)]

        con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
        metadata_for_all_contracts = pd.read_sql("select distinct(longname) from all_contracts_metadata",
                                                 con)
        all_headlines = pd.read_sql("select description from news_headlines", con)
        headlines_and_metadata = pd.concat([metadata_for_all_contracts.longname, all_headlines.description])

        flatten = lambda l: [item for sublist in l for item in sublist]
        words = flatten([re.findall('\w+', x[0]) for x in headlines_and_metadata.values])
        preprocessed_words = [self.pre_process(word) for word in words]

        word_counts = Counter(preprocessed_words)
        top_3_percent = int(floor(len(word_counts.items())*.03))
        common_words_w_counts = word_counts.most_common(top_3_percent)

        # Combine new stop words with numbers!
        new_stop_words = [word for word, count in common_words_w_counts] + nums
        stops = [w.encode('ascii', 'ignore') for w in nltk_stopwords + new_stop_words]

        return stops


    def pre_process(self, text):
        """ Converts apostrophes and other problematic elements to double underscores for
            MySQL friendliness. """

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


    def get_contract_metadata(self, contract_ticker):
        """ Pulls all data for contract, up to limit rows. """

        con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
        sql_statement = "select * from all_contracts_metadata where contract_ticker='{}' limit 1".format(contract_ticker)
        df = pd.read_sql(sql_statement, con)

        return df


    def strip_stopwords(self, text):
        """ Takes in some text (this will be the longname or a news headline) and a list of stopwords.
            Removes stopwords from text. Returns remaining words in text as list.

            Number 0-100 and 2000 - 2020 are stopwords. """

        text = str(text).encode('ascii', 'ignore')
        split_preprocessed = [self.pre_process(word) for word in text.split()]
        return [w for w in split_preprocessed if w not in self.stopwords]


    def pull_headlines(self, reference_time):
        """ Pulls headlines that are within 30 minutes of reference_time (datetime object that
            represents when the contract data was pulled). """

        con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
        time_cutoff = reference_time - timedelta(minutes = 500)
        sql_statement = """select * from (select * from news_headlines
                           where record_timestamp > '{0}') a
                           where a.article_timestamp > '{0}' """.format(time_cutoff)
        df = pd.read_sql(sql_statement, con)
        return df


    def find(self, contract_ticker, reference_time = datetime.now()):
        """ Pulls contract longname and looks for a headline that might match it within 30 mins
            of the reference time. """

        longname = self.get_contract_metadata(contract_ticker).longname.values
        longname_words = self.strip_stopwords(longname)
        hlines = self.pull_headlines(reference_time)

        print longname, longname_words

        # Get the size of the intersection between the stopword-less longname
        # (contract description) and each headline from the last 30 mins.
        scores = []
        actual_sets = []

        for key, line in hlines.iterrows():
            # Use the actual headline, not description:
            description = self.strip_stopwords(line['headline'])

            scores.append(len(set(longname_words).intersection(set(description))))
            actual_sets.append(set(longname_words).intersection(set(description)))

        hlines['scores'] = scores
        hlines['actual_sets'] = actual_sets

        self.last_contract_description = str(longname)

        return hlines[hlines.scores > 0]
