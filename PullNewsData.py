#!/usr/bin/env python

# Set up some scripts to pull newswire data into a database of headlines
# Defining some functions to pull data from a news api.
import pickle
import requests
from datetime import datetime
import sqlalchemy as sqa
import pandas as pd
import hashlib

with open('/home/ec2-user/predictit_assistant/news_sources.p', 'r') as infile:
    news_sources = pickle.load(infile)

def sql_friendly(text):
    """ Converts apostrophes to double underscores for MySQL friendliness. """
    if not text or text == '':
        return 'missing__data'.encode('ascii', 'ignore')
    try:
        return text.replace("'", "__").replace(",", "__").replace('"', "__").replace("%", "__").encode('ascii', 'ignore')
    except:
        return text.encode('ascii', 'ignore')

def get_news_data(source):
    """ Pulls headlines from the newsapi.org api for a given news source. """
    key = "1a98f26fb9f342d2a15e9e58fb14fd2c"
    url_base = "https://newsapi.org/v1/articles?apikey"
    endpoint = "{}={}&source={}".format(url_base, key, source)

    headlines = []

    for art in requests.get(endpoint).json()['articles']:

        description = lambda x: x['description'] if 'description' in x else 'no_desc_found'
        title = art['title']
        headline_unique_id = hashlib.sha224(title.encode('ascii', 'ignore')).hexdigest()

        headlines.append((source, title, art['publishedAt'], description(art),
                          headline_unique_id))

    return headlines


def send_to_sql(source, con):
    """ Gets news data, checks if each headline already exists in database using the
        unique headline id. If it doesn't, adds it to the database. """

    news_dat = get_news_data(source)

    for headline in news_dat:

        if type(headline) is not tuple or len(headline) != 5:
            return

        _, title, article_timestamp, description, unique_id = headline
        title = sql_friendly(title)
        description = sql_friendly(description)
        unique_id = sql_friendly(unique_id)
        record_timestamp = sql_friendly(str(datetime.now()))

        sql = """SELECT unique_id FROM news_headlines WHERE unique_id='%s' LIMIT 1""" % (str(unique_id))
        df = pd.read_sql(sql, con)

        try:
            if len(df) == 0:
                sql = """INSERT INTO news_headlines VALUES ('{0}', '{1}', '{2}', '{3}', '{4}', '{5}')""".format(record_timestamp,
                                                                                                                source,
                                                                                                                article_timestamp,
                                                                                                                title,
                                                                                                                unique_id,
                                                                                                                description
                                                                                                               )
                con.execute(sql)

        except:
            continue


if __name__ == "__main__":
    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    for s in news_sources:
        send_to_sql(s, con)
