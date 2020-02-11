#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 19:51:54 2019

@author: rimzimthube
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  7 16:31:30 2019

@author: rimzimthube
"""
from textblob import TextBlob
from sklearn.metrics import accuracy_score,confusion_matrix,f1_score,precision_score,recall_score
import re
from nltk.tokenize import word_tokenize
from string import punctuation 
from nltk.corpus import stopwords 
from textblob.sentiments import NaiveBayesAnalyzer
from textblob.classifiers import NaiveBayesClassifier
from textblob import Blobber



NBAnalyzer = Blobber(analyzer=NaiveBayesAnalyzer())   



'''
Pre process the text 
'''
def processText(text):
    
    _stopwords = set(stopwords.words('english') + list(punctuation) + ['AT_USER','URL'])


    tweet = text.lower() # convert text to lower-case
    tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet) # remove URLs
    tweet = re.sub('@[^\s]+', 'AT_USER', tweet) # remove usernames
    tweet = re.sub(r'#([^\s]+)', r'\1', tweet) # remove the # in #hashtag
    tweet = word_tokenize(tweet) # remove repeated characters (helloooooooo into hello)
    tweet=[word for word in tweet if word not in _stopwords]
    
    tweet=' '.join(tweet)
    return tweet

'''
Build training set from the CSV file
'''
 
def buildTrainSet(corpusFile):
    import csv

    trainingDataSet=[]
#    naiveBayesTrainSet=[]
    
    with open(corpusFile,'rt',encoding='latin-1') as csvfile:
        lineReader = csv.reader(csvfile, delimiter=',', quotechar="\"")
        for row in lineReader:
#            trainingDataSet.append({"tweet_id":row[0], "label":row[2], "topic":row[3],"text":row[1]})
            label=row[0]
            if(label=='0'):
                trainingDataSet.append({"label":'negative', "text":row[5]})
            elif(label=='2'):
                trainingDataSet.append({"label":'neutral', "text":row[5]})
            elif(label=='4'):
                trainingDataSet.append({"label":'positive', "text":row[5]})
#            row=row[1],row[2]
#            naiveBayesTrainSet.append(row)
            
    return trainingDataSet


'''
Get sentiment for the text
'''          
def getSentiment(trainSet):
    
    sentimentTweetBlob=[]
    sentimentFile=[]
    
    postiveBlob=0
    negativeBlob=0
    neutralBlob=0
    postiveFile=0
    negativeFile=0
    neutralFile=0
    
    counter=0

    for tweet in trainSet:
        text=tweet["text"]
        
        text=processText(text)
        
        if(tweet["label"]!='irrelevant'):
            
            sentimentFile.append(tweet["label"])
#            if(tweet["label"]=='positive'):
#                postiveFile=postiveFile+1
#            elif(tweet["label"]=='negative'):
#                negativeFile=negativeFile+1
#            elif(tweet["label"]=='neutral'):
#                neutralFile=neutralFile+1
            
            analysis =  NBAnalyzer(text) 
            counter=counter+1
            print(counter)
            if analysis.sentiment.p_pos>analysis.sentiment.p_neg:
                sentimentTweetBlob.append('positive')        
#                postiveBlob=postiveBlob+1
            elif analysis.sentiment.p_pos<analysis.sentiment.p_neg:
                sentimentTweetBlob.append('negative')       
#                negativeBlob=negativeBlob+1
#            elif analysis.sentiment.p_neg<0.2:  
#                sentimentTweetBlob.append('neutral')
            else:   
                sentimentTweetBlob.append('neutral')
            
#            analysis = TextBlob(text)    
#            if analysis.sentiment[0]>0:  
#                sentimentTweetBlob.append('positive')
#                postiveBlob=postiveBlob+1
#            elif analysis.sentiment[0]<0:   
#                sentimentTweetBlob.append('negative')
#                negativeBlob=negativeBlob+1
#            else:   
#                sentimentTweetBlob.append('neutral')
                neutralBlob=neutralBlob+1
                
                
#                neutralBlob=neutralBlob+1
    accuracy=accuracy_score(sentimentFile,sentimentTweetBlob)
    print('Accuracy - '+format(accuracy))
    
    confusion=confusion_matrix(sentimentFile,sentimentTweetBlob,labels=["positive", "negative", "neutral"])
    print('Confusion matrix - \n'+format(confusion))
    
    f1score=f1_score(sentimentFile, sentimentTweetBlob,labels=["positive", "negative", "neutral"],average='macro')
    print('F1 score - '+format(f1score))
    
    precisionScore=precision_score(sentimentFile, sentimentTweetBlob, average='macro')
    print('Precision Value - '+format(precisionScore))
    
    recall= recall_score(sentimentFile, sentimentTweetBlob, average='macro')
    print('Recall - '+format(recall))
            
#    print('postiveBlob - '+format(postiveBlob))
#    print('negativeBlob - '+format(negativeBlob))
#    print('neutralBlob - '+format(neutralBlob))
#    print('total - '+format(neutralBlob+postiveBlob+negativeBlob))
#    print('postiveFile - '+format(postiveFile))
#    print('negativeFile - '+format(negativeFile))
#    print('neutralFile - '+format(neutralFile))
#    print('total - '+format(postiveFile+negativeFile+neutralFile))


corpusFile = "/Users/rimzimthube/Downloads/trainingandtestdata/training.csv"

trainSet=buildTrainSet(corpusFile)
#print('trainset created')
#cl=NaiveBayesClassifier(trainSet)
#print('classifier created')
#result=cl.classify("I feel amazing!")
#print(result)

print(trainSet[0:4])

getSentiment(trainSet)

#analysis = TextBlob('Python is a high-level programming language.',analyzer=NaiveBayesAnalyzer())    
#print(analysis.sentiment)
# 
#analysis1 = TextBlob('Python is a high-level programming language.')    
#print(analysis1.sentiment)
 


