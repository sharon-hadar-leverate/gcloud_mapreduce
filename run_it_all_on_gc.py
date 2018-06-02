from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.tokenize import RegexpTokenizer
from google.cloud import bigtable
from google.cloud import happybase
from subprocess import call
from geopy.geocoders import Nominatim

import numpy as np
import pandas as pd
import tweepy
import re
import datetime
import os
import os.path
import argparse
import sys

# ===============   Global variables
query = 'trump'
word_tokenizer = RegexpTokenizer('[a-zA-Z]\w+')
Qey_words = word_tokenizer.tokenize(query.lower())
location_list = [['London']]
df_results = pd.DataFrame()

# Define start date of our word survey
delta = -30
start_date = datetime.datetime.now() + datetime.timedelta(delta)
start_date = str(start_date.year) + '-' + str(start_date.month) + '-' + str(start_date.day)

s_words = {'ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about',
           'once', 'during', 'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be',
           'some', 'for', 'do', 'its', 'yours', 'de', 'vs', 'such', 'into', 'of', 'most', 'itself',
           'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each',
           'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his',
           'through', 'don', '\'\'', 'nor', 'me', 'were', 'her', 'more', 'himself',
           'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both',
           'up', 'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any',
           'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on',
           'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why',
           'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has',
           'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after',
           'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by',
           'doing', 'it', 'how', 'further', 'was', 'here', 'than', 'The',
           'rt', '&amp', ' ', '', '``', 'http', 'via', 'https', 'amp', '\'s'}


def tw_oauth():
    consumer_key = u'5KHfdzjZpsSef0pDjHSIUqxkb'
    consumer_secret = u'HiKeFDDaYK1iY3R3nnRVZOfPVDNuYySm0MXDYV2kS878xIAvSP'
    access_token = u'762963428973617152-snH3bENqYdoN4MRi8d02GrxFUO9Gi1O'
    access_token_secret = u'COoonSphtHDQtcArpdJbg1q0CLozML4yZS91pbenuk5yL'
    ak = (consumer_key, consumer_secret, access_token, access_token_secret)
    auth1 = tweepy.auth.OAuthHandler(ak[0].replace("\n", ""), ak[1].replace("\n", ""))
    auth1.set_access_token(ak[2].replace("\n", ""), ak[3].replace("\n", ""))
    return tweepy.API(auth1)


def tokenize_tweet_text(tweet_text, Qye_words=None):
    word_tokens = word_tokenizer.tokenize(tweet_text)
    filtered_sentence = []

    for w in word_tokens:
        if (len(w) <= 1): continue
        if w.lower() not in (s_words | set(Qye_words)):
            filtered_sentence.append(w.lower())

    return filtered_sentence

geolocator = Nominatim()
def get_coord(location="New york", i=3):
    try:
        location = geolocator.geocode(location)
        return "{},{},{}".format(location[1][0], location[1][1], "50km")

    except:
        if i >= 0:
            print("retry for time:", 3 - i)
            return get_coord(location=location, i=i - 1)
        else:
            print("could not find location: ", location)
            return None


def tw_get_tweets(api, query_in, Qye_words, location, num_tweets=200):
    counter = 0
    word_list = []
    cs = tweepy.Cursor(api.search,
                       q=query_in,  # the actual words we search
                       geocode=get_coord(location),  # location
                       since=start_date,
                       count=num_tweets).items()

    while True:
        try:
            tweet = cs.next()
            text = tweet.text  # tweet text
            word_list += tokenize_tweet_text(text, Qye_words=Qye_words)

            counter += 1
            if counter >= num_tweets:
                break
        except tweepy.TweepError:
            break
        except StopIteration:
            break

    print('	******************** counter of collected tweets: ', counter)
    return word_list


def get_tweets_words(location_words, query_in):
    print('Retreiving tweets since: ', start_date, ' about: ')
    print(query_in)
    print('Tweeted in locations: ', location_words[0])
    fdir = location_words[0].replace(" ", "_")
    api = tw_oauth()

    words = tw_get_tweets(api, query_in, [x.lower() for x in Qey_words],
                          location=[x.lower() for x in location_words],
                          num_tweets=2000)

    df_city = pd.DataFrame(index=np.arange(0, len(words)), columns=[[fdir]],
                           data=words)
    return df_city


def load_csv_to_bucket(df):
    bucket_name = os.environ.get('BUCKET_NAME', app_identity.get_default_gcs_bucket_name())
    self.response.headers['Content-Type'] = 'text/plain'
    self.response.write('Demo GCS Application running from Version: ' + os.environ['CURRENT_VERSION_ID'] + '\n')
    self.response.write('Using bucket name: ' + bucket_name + '\n\n')


def get_max_val(table, no_keyes):
    key_max = ''
    val_max = 0
    i = 0
    for key, row in table.scan():
        if key not in no_keyes:
            val = int.from_bytes(row[b'cf:count'], byteorder='big')
            if val > i:
                key_max = key.decode("utf-8")
                val_max = val
                i = val
    return key_max, val_max


if __name__ == '__main__':
    n_times = 1
    df = []
    for i in range(n_times):
        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument('-n', '--locations', nargs='+', default=location_list[i],
                            help=" (default location: %(default)s)")
        parser.add_argument('-q', '--about', default=query, help=" (default Query: %(default)s)")
        args = parser.parse_args()
        query = args.about
        Qey_words = word_tokenizer.tokenize(query)
        print('Folllowing words will be not counted: ', Qey_words)
        print('args.locations', args.locations)
        query_list = args.locations

        df = get_tweets_words(args.locations, args.about)
        print()
        print()
        print('Example of words in Tweets about:')
        print(query)
        print()

        print(df.head())

        csv_file = df.to_csv('tweet_words.txt', sep=' ', index=False, header=False)
        print('Running: gsutil ls -l gs://sharon-bucket/tweet')
        os.system('gsutil ls -l gs://sharon-bucket/tweet')
        print('Running: gsutil rm  -f  gs://sharon-bucket/tweet/*')
        os.system("gsutil rm  -f  gs://sharon-bucket/tweet/tweet_words.txt")
        print('Running: gsutil cp tweet_words.txt gs://sharon-bucket/tweet')
        os.system("gsutil cp tweet_words.txt gs://sharon-bucket/tweet")
        print('Running: gsutil ls -l gs://sharon-bucket/tweet')
        os.system('gsutil ls -l gs://sharon-bucket/tweet')

        run_job = 'gcloud dataproc jobs submit hadoop \
        --cluster sharon-mapreduce-bigtable \
        --jar target/wordcount-mapreduce-1.0-jar-with-dependencies.jar \
        -- wordcount-hbase \
        gs://sharon-bucket/tweet \
        "tweet-words-count"'

        print('Running mapreduce job: ')
        print(run_job)

        os.system(run_job)

    print('=======================================================================================')

    table_name = "tweet-words-count"
    project_id = 'sharon-project-204821'
    instance_id = 'sharon-mapreduce-bigtable'
    column_family_name = 'cf'
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    connection = happybase.Connection(instance=instance)


    def ByteToHex(byteStr):
        return ''.join(["%02X " % ord(x) for x in byteStr]).strip()


    try:
        table = connection.table(table_name)
        print('scanning <', table_name, '>')
        word_freq = []

        for key, row in table.scan():
            byte_string = row[b'cf:count']
            val = int("0x" + ''.join([hex(x)[2:] for x in byte_string]), base=16)
            word_freq.append((val, str(key)[2:-1]))

        word_freq.sort(reverse=True)
        print(word_freq[0:5])

        print('Deleting the {} table.'.format(table_name))
        connection.delete_table(table_name)

    finally:
        connection.close()
