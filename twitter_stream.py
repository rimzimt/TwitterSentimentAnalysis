

'''
Command used in terminal:
python MS/LSA/Project/Working3/WebSiteProducer.py


'''

import socket
import sys
import requests
import requests_oauthlib
import json
import argparse
from textblob import TextBlob
import csv
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords 
from string import punctuation 
from pymongo import MongoClient
import datetime
import nltk
from textblob.sentiments import NaiveBayesAnalyzer
from textblob import Blobber


twitterdb=None
tweetCollection=None
fileName= '/Users/rimzimthube/MS/LSA/Project/SavedCSV.csv'
trend_var = ''

'''Twitter API connection strings 
'''
access_token = '1189013439777144834-gHdx56y8wdFEXx6on8RvqKFd7jZWgr'
access_secret = 'Xr8IgYAZwbcwOuKOkCogcahAjpZPGz1zOzcEO27cbCqr2'
consumer_key = 'LSrCCGTL3XXvVxUaeMJ1P9kvQ'
consumer_secret = 'dVvm1uFcePm0o0M6fxJg3vxqouoB3o0HOOWlzwhElleH1dOpGj'

# Mongo DB conection string 
mongoclient = MongoClient('mongodb+srv://rimzimt:Rimzim!123@cluster0-x8z3i.mongodb.net/test?retryWrites=true&w=majority')

# Connect to Twitter 
my_auth = requests_oauthlib.OAuth1(consumer_key, consumer_secret,access_token, access_secret)

# Naive Bayes Analyzer
NBAnalyzer = Blobber(analyzer=NaiveBayesAnalyzer())   

'''
Pre-process the data
'''
def processText(text):
    _stopwords = set(stopwords.words('english') + list(punctuation) + ['AT_USER','URL'])
    
    emoji_pattern = re.compile("["
         u"\U0001F600-\U0001F64F"  # emoticons
         u"\U0001F300-\U0001F5FF"  # symbols & pictographs
         u"\U0001F680-\U0001F6FF"  # transport & map symbols
         u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
         u"\U00002702-\U000027B0"
         u"\U000024C2-\U0001F251"
         "]+", flags=re.UNICODE)

    # happy emoticons
    happy_emoticons = set([
    ':-)', ':)', ';)', ':o)', ':]', ':3', ':c)', ':>', '=]', '8)', '=)', ':}',
    ':^)', ':-D', ':D', '8-D', '8D', 'x-D', 'xD', 'X-D', 'XD', '=-D', '=D',
    '=-3', '=3', ':-))', ":'-)", ":')", ':*', ':^*', '>:P', ':-P', ':P', 'X-P',
    'x-p', 'xp', 'XP', ':-p', ':p', '=p', ':-b', ':b', '>:)', '>;)', '>:-)',
    '<3'
    ])

    # sad emoticons
    sad_emoticons = set([
    ':L', ':-/', '>:/', ':S', '>:[', ':@', ':-(', ':[', ':-||', '=L', ':<',
    ':-[', ':-<', '=\\', '=/', '>:(', ':(', '>.<', ":'-(", ":'(", ':\\', ':-c',
    ':c', ':{', '>:\\', ';('
    ])
    
    # club the emoticons
    emoticons = happy_emoticons.union(sad_emoticons)

    processedText = text.lower() # convert text to lower-case
    processedText = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', processedText) # remove URLs
    processedText = re.sub('@[^\s]+', 'AT_USER', processedText) # remove usernames
    processedText = re.sub(r'#([^\s]+)', r'\1', processedText) # remove the # in #hashtag

    processedText = re.sub(r':', '', processedText) # preprocessing the colon symbol left remain after 
    processedText = re.sub(r'‚Ä¶', '', processedText) #removing mentions
    processedText = re.sub(r'[^\x00-\x7F]+',' ', processedText) ##replace consecutive non-ASCII characters with a space
    processedText = emoji_pattern.sub(r'', processedText)

    processedText = word_tokenize(processedText) # remove repeated characters (helloooooooo into hello)
    processedText=[word for word in processedText if word not in _stopwords and word not in emoticons]

    processedText=' '.join(processedText)
    return processedText

'''
get the sentiment of the text
'''

def GetTweetSentiment(tweet_text):
    
    # pre-process the tweet text
    tweet_text = processText(tweet_text)
    
#    analysis = TextBlob(tweet_text,analyzer=tb)  
    
    # apply Naive Bayes algorothm to detect the sentiment 
    analysis=NBAnalyzer(tweet_text)
    print(analysis.sentiment)
    
    # categorise the sentiment
    if analysis.sentiment.p_pos>analysis.sentiment.p_neg:
        sentiment='Positive'
    elif analysis.sentiment.p_pos<analysis.sentiment.p_neg:
        sentiment='Negative'
    else:
        sentiment='Neutral'
         
#    analysis = TextBlob(tweet_text)
#    print(analysis.sentiment)
#    if analysis.sentiment[0]>0:
#        sentiment='Positive'
#    elif analysis.sentiment[0]<0:
#        sentiment='Negative'
#    else:
#        sentiment='Neutral'

        
    return sentiment

'''
get tweets from twitter api
'''
def get_tweets():
    
    global trend_var
    
    # twitter api url
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    # query_data = [('language', 'en'), ('tweet_mode','extended'), \
            # ('locations', '-130,-20,100,50'),('track','trump')]
    # query string  
    query_data = [('language', 'en'), ('tweet_mode','extended'), \
            ('track', trend_var)]
    
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + \
        str(t[1]) for t in query_data])
    
    response = requests.get(query_url, auth=my_auth, stream=True)
    
    print(query_url, response)
    
    return response

'''
send the tweet data to Spark
'''
def send_tweets_to_spark(http_resp, tcp_connection):
    
    global tweetCollection
    
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            if 'truncated' not in full_tweet.keys():
                print(full_tweet.keys())
                continue
            
            extended=full_tweet['truncated']

            #More than 140 chars
            if(extended==True):
                if 'retweeted_status' in full_tweet: #retweet
                    
                    tweet_text=full_tweet['retweeted_status']\
                            ['extended_tweet']['full_text']

                else: #Normal Tweet
                    
                    tweet_text=full_tweet['extended_tweet']['full_text']
            #140 chars
            elif (extended==False):
                #If normal tweet is RT
                if 'retweeted_status' in full_tweet:
                    if(full_tweet['retweeted_status']['truncated']==True):
                        
                        tweet_text=full_tweet['retweeted_status']['extended_tweet']['full_text']
                    else:
                        
                        tweet_text=full_tweet['retweeted_status']['text']
                else:
                    
                    tweet_text=full_tweet['text']
               
            
            sentiment=GetTweetSentiment(tweet_text)
            
            # insert the data in MongoDB
            mongoTweet={'tweet':tweet_text,'sentiment':sentiment,'hashtag':trend_var,'DateTime':datetime.datetime.now()}
            tweetCollection.insert_one(mongoTweet)
            
            # send tweet data over tcp connection
            tcp_connection.send((sentiment+'\n').encode())
            print("Tweet Sent!")
        except (LookupError, NameError):
            e = sys.exc_info()[0]
            print(tweet_text)
            print("Error: %s" % e)
            exit(0)
        except(ConnectionResetError, BrokenPipeError):
            print("Client disconnected ..")
            return
        except(json.decoder.JSONDecodeError):
            continue
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

'''
input arguments
'''
def argsStuff():
    
    parser = argparse.ArgumentParser(description = "Twitter sentiment analysis")
    parser.add_argument("-V", "--version", help="show program version", \
            action="store_true")
    parser.add_argument("-w", "--word", help="hashtag for sentiment analysis",\
            type=str)
    args = parser.parse_args()
    if args.version:
        print("V1.1")
        exit(0)
    if not args.word:
        print("The following arguments are required: -w/--word")
        exit(1)
    return args.word

def main():
    
    global trend_var
    global tweetCollection
    global twitterdb
    
    # nltk.download('stopwords')
    trend_var = argsStuff()
    print(trend_var)
    
    TCP_IP = "localhost"
    TCP_PORT = 9999
    
    twitterdb=mongoclient['twitterdb']
    tweetCollection=twitterdb['tweetcollection']

    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    while True:
        s.listen(1)
        print("Waiting for TCP connection...")
        conn, addr = s.accept()
        print("Connected... Starting getting tweets.")
        # print(conn)
        resp = get_tweets()
        send_tweets_to_spark(resp, conn)

if __name__ == '__main__':
    main()