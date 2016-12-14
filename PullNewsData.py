# Set up some scripts to pull newswire data into a database of headlines
# Defining some functions to pull data from a news api.
import pickle
import requests

with open('/home/ec2-user/predictit_assistant/news_sources.p', 'r') as infile:
    news_sources = pickle.load(infile)

def get_news_data(source):
    """ Pulls headlines from the newsapi.org api for a given news source. """
    key = "1a98f26fb9f342d2a15e9e58fb14fd2c"
    url_base = "https://newsapi.org/v1/articles?apikey"
    endpoint = "{}={}&source={}".format(url_base, key, source)

    headlines = []

    for art in requests.get(endpoint).json()['articles']:
        headlines.append((source, art['title'], art['publishedAt']))

    return headlines
