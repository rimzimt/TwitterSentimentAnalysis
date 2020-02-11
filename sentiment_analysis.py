

'''
Command used in terminal:
spark-submit MS/LSA/Project/Test/untitled7.WebsiteConsumer.py localhost 9999


'''

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import argparse

ip="localhost"
#ip="172.31.80.177"

def aggregate_sentiment_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
    return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
    print("----------- %s -----------" % str(time))
    try:
        
        sql_context = get_sql_context_instance(rdd.context)
        
        
        row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
        
        # create a df from row rdd
        sentiment_df = sql_context.createDataFrame(row_rdd)
        
        # Register df as table
        sentiment_df.registerTempTable("hashtags")
        
        # get the sentiment count
        sentiment_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
        print('count')
        
        top_sentiment = [str(t.hashtag) for t in sentiment_counts_df.select("hashtag").collect()]
        sentiment_count = [p.hashtag_count for p in sentiment_counts_df.select("hashtag_count").collect()]
        
        print(top_sentiment)
        print(sentiment_count)
        
        # send data to chart 
        send_df_to_dashboard(sentiment_counts_df)
        
    except (ValueError):
        print('')
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
        
def process_string(time,rdd):
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        
        row_rdd = rdd.map(lambda w: Row(Tweet=w))
        
        frame=row_rdd.toDF()
        
        print("createddataframe")
        print(format(frame))
        
        
        
        send_df_to_dashboard(sentiment_counts_df)
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def send_df_to_dashboard(df):
    
    
    # extract the sentiment from dataframe and convert them into array
    top_sentiment = [str(t.hashtag) for t in df.select("hashtag").collect()]
    
    # extract the counts from dataframe and convert them into array
    sentiment_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]
    
    # initialize and send the data through REST API
    #url = 'http://localhost:5001/updateData'
    url = 'http://'+ip+':5001/updateData'
    request_data = {'label': str(top_sentiment), 'data': str(sentiment_count)}
    
    try:
        response = requests.post(url, data=request_data)
    except (requests.exceptions.ConnectionError):
        print()

def argsStuff():
    
    parser = argparse.ArgumentParser(description = "Fetch tweets")
    parser.add_argument("-V", "--version", help="show program version", \
            action="store_true")
    parser.add_argument("-i", "--ip", help="ip address to publish chart",\
            type=str)
    args = parser.parse_args()
    if args.version:
        print("V1.1")
        exit(0)
    if not args.ip:
        print("The following arguments are required: -i/--ip")
        exit(1)
    return args.ip

# create spark configuration
# ip=argsStuff()
conf = SparkConf().setMaster("local[2]").setAppName("TwitterStreamApp")

sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

ssc = StreamingContext(sc, 3)

ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9999
dataStream = ssc.socketTextStream("localhost",9999)
print("datastream")


words = dataStream.flatMap(lambda line: line.split("\n"))

hashtags = words.map(lambda x: (x, 1))

tags_totals = hashtags.updateStateByKey(aggregate_sentiment_count)

tags_totals.foreachRDD(process_rdd)

ssc.start()
ssc.awaitTermination()