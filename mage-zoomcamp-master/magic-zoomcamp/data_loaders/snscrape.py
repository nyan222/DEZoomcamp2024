import pandas as pd 
import numpy as np 
import snscrape.modules.twitter as sntwitter 
import datetime 
from tqdm.notebook import tqdm_notebook


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
### inspiration
### https://datasciencedojo.com/blog/scrape-twitter-data-using-snscrape/#
### https://medium.com/@noumanmustafa741/getting-data-from-twitter-using-snscrape-d6f8f4243d8b


#the query for searchinf
text = 'roman empire'
#if we would like to find soome personal tweets
username = ''
#dates
since = '2023-09-01'
until = '2023-09-30'
#max number of tweets or enter -1 to retrieve all possible tweets
count = 10
#min_faves - min_likes
min_faves = 22
#Exclude Retweets? (y/n)
retweet = 'y'
#Exclude Replies? (y/n)
replies = 'y'

def search(text,username,since,until,retweet,replies,min_faves): 
    global filename 
    q = text 
    if username!='': 
        q += f" from:{username}"
    if until=='': 
        until = datetime.datetime.strftime(datetime.date.today(), '%Y-%m-%d') 
    q += f" until:{until}" 
    if since=='': 
        since = datetime.datetime.strftime(datetime.datetime.strptime(until, '%Y-%m-%d') - datetime.timedelta(days=7), '%Y-%m-%d') 
    q += f" since:{since}" 
    if retweet == 'y': 
        q += f" exclude:retweets" 
    if replies == 'y': 
        q += f" exclude:replies" 
    # if username!='' and text!='': 
    #     filename = f"{since}_{until}_{username}_{text}.csv" 
    # elif username!="": 
    #     filename = f"{since}_{until}_{username}.csv" 
    # else: 
    #     filename = f"{since}_{until}_{text}.csv" 
    # print(filename) 
    return q 
q = search(text,username,since,until,retweet,replies,min_faves)     

@data_loader
def load_data(*args, **kwargs):
    tweets_list1 = [] 
    # Using TwitterSearchScraper to scrape data and append tweets to list 
    if count == -1: 
        for i,tweet in enumerate(tqdm_notebook(sntwitter.TwitterSearchScraper(q).get_items())): 
            tweets_list1.append([tweet.date, tweet.id, tweet.rawContent, tweet.user.username,tweet.lang, tweet.hashtags,tweet.replyCount,tweet.retweetCount, tweet.likeCount,tweet.quoteCount,tweet.media,tweet.url,tweet.date.year]) 
    else: 
        with tqdm_notebook(total=count) as pbar: 
            for i,tweet in enumerate(sntwitter.TwitterSearchScraper(q).get_items()): #declare a username  
                if i>=count: #number of tweets you want to scrape 
                    break 
                tweets_list1.append([tweet.date, tweet.id, tweet.rawContent, tweet.user.username,tweet.lang, tweet.hashtags,tweet.replyCount,tweet.retweetCount, tweet.likeCount,tweet.quoteCount,tweet.media,tweet.url,tweet.date.year]) 
                pbar.update(1) 
    data = pd.DataFrame(tweets_list1, columns=['DateTime', 'TweetId', 'Text', 'Username','Language','Hashtags','ReplyCount','RetweetCount','LikeCount','QuoteCount','Media','URL','Year']) 

    return {data}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
