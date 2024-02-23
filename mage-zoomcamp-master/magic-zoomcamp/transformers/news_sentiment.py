import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    df = pd.DataFrame(data, columns=['id','date','edition','page','file_name','word_count', 'text','year'])
    news_list = []
    for index, news in df.iterrows():
        #print(news)
        analyzer = SentimentIntensityAnalyzer().polarity_scores(news.text)
        neg = analyzer['neg']
        neu = analyzer['neu']
        pos = analyzer['pos']


        if neg > pos:
            news['sent'] = 'neg'
        elif pos > neg:
            news['sent'] = 'pos'
        elif pos == neg:
            news['sent'] = 'neu'
        #print(news)
        news_list.append([news.id,news.date,news.edition,news.page,news.file_name,news.word_count, news.text,news.year, news.sent])
    data = pd.DataFrame(news_list, columns=['id','date','edition','page','file_name','word_count', 'text','year','sent'])
    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
