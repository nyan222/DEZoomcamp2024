from mage_ai.data_preparation.variable_manager import set_global_variable
import pyarrow.parquet as pq
import pandas as pd
import nltk
nltk.download('punkt')
from nltk import bigrams, word_tokenize

##set_global_variable('news_to_gcs', 'var22', 0)

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

init_url = 'https://huggingface.co/datasets/PleIAs/US-PD-Newspapers/resolve/refs%2Fconvert%2Fparquet/default/train'

@data_loader
def load_data(*args, **kwargs):
    news_list=[]
    ##set_global_variable('news_to_gcs', 'var22', 0)
    print('var22='+ str(kwargs.get('var22')))
    i = kwargs.get('var22')
    print(i)
    n = i+50
    if n > 2618:
        n = 2618
    print(n)
    while n > i:
        num = '000'+str(i)
        num = num[-4:]
        file_name = f"{num}.parquet"
        request_url = f"{init_url}/{file_name}"
        print(request_url)
        df = pd.read_parquet(request_url,engine='pyarrow')
        for index, line in df.iterrows():
            # Tokenize the text
            tokens = word_tokenize(line['text'].lower())
            bi_grams = list(bigrams(tokens))
            if ('roman', 'empire') in bi_grams:
                news_list.append([line.id,line.date,line.word_count, line.text])

        i = i+1
    data = pd.DataFrame(news_list, columns=['id','date','word_count', 'text']) 
    
    set_global_variable('news_to_gcs', 'var22', n)
    print('var22 in the end = '+ str(kwargs.get('var22')))
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your data loading logic here

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
