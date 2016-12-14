# Set up some scripts to pull newswire data into a database of headlines
# Defining some functions to pull data from a news api.
import pickle
import requests
from datetime import datetime

with open('/home/ec2-user/predictit_assistant/news_sources.p', 'r') as infile:
    news_sources = pickle.load(infile)

def sql_friendly(text):
    """ Converts apostrophes to double underscores for MySQL friendliness. """
    try:
        return text.replace("'","__")
    except:
        return text

def get_news_data(source):
    """ Pulls headlines from the newsapi.org api for a given news source. """
    key = "1a98f26fb9f342d2a15e9e58fb14fd2c"
    url_base = "https://newsapi.org/v1/articles?apikey"
    endpoint = "{}={}&source={}".format(url_base, key, source)

    headlines = []

    for art in requests.get(endpoint).json()['articles']:

        description = lambda x: x['description'] if 'description' in x else 'none'
        title = art['title']
        headline_unique_id = str(art['publishedAt']) + str(source[:min(5, len(source)/2)]) + title[:min(6, len(title)/2)]

        headlines.append((source, title, art['publishedAt'], description(art),
                          headline_unique_id))

    return headlines


def send_to_sql(source, con):
    """ Gets news data, checks if each headline already exists in database using the
        unique headline id. If it doesn't, adds it to the database. """

    for headline in get_news_data(source):
        _, title, article_timestamp, description, unique_id = headline
        title = sql_friendly(title)
        description = sql_friendly(description)
        unique_id = sql_friendly(unique_id)

        sql_statement = "select unique_id from news_headlines where unique_id='{}' limit 1".format(unique_id)
        df = pd.read_sql(sql_statement, con)
        if len(df) > 0:
            continue

        con.execute("INSERT INTO news_headlines VALUES ('{}', '{}', '{}', '{}', '{}', '{}')".format(datetime.now(),
                                                                                        source,
                                                                                        article_timestamp,
                                                                                        title,
                                                                                        unique_id,
                                                                                        description))


if __name__ == "__main__":
    con = sqa.create_engine('mysql+mysqldb://root:@localhost/predictit_db').connect()
    for s in news_sources:
        send_to_sql(s, con)
