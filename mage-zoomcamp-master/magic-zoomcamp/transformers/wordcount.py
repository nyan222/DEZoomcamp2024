import string
import re
import nltk
#nltk.download('word_tokenize')
nltk.download('stopwords')
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
from nltk.corpus import stopwords
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print(data.shape)
    df = data.groupby('year').agg({'text': ' '.join}).reset_index()

    # Convert text to lowercase
    df['text'] = df['text'].str.lower()

    # Remove special characters and digits
    spec_chars = string.punctuation + '\t'
    df['text'] = df['text'].apply(lambda x: ''.join([ch for ch in x if ch not in spec_chars]))
    df['text'] = df['text'].apply(lambda x: re.sub('\n', ' ', x))
    df['text'] = df['text'].apply(lambda x: ''.join([ch for ch in x if ch not in string.digits]))
    print(df.head(2))
    #tokenize and clean again
    df['text_tokens'] = df['text'].apply(lambda x: word_tokenize(x))
    eng_stopwords = stopwords.words("english")
    df['text_tokens'] = df['text_tokens'].apply(lambda x: [token.strip() for token in x if token not in eng_stopwords])
    df['text_tokens'] = df['text_tokens'].apply(lambda x: [token.strip() for token in x if len(token) > 2])
    #df['text_tokens'] = df['text_tokens'].apply(lambda x: nltk.Text(df['text_tokens']))
    print(df.head(2))
    # Calculate the frequency distribution
    df['fdist'] = df['text_tokens'].apply(lambda x: nltk.FreqDist(x).most_common(20))

    # Drop the unnecessary columns and display the first 2 rows
    df = df.drop(columns=['text', 'text_tokens'])
    print(df.head(2))

    # Create a new DataFrame for word count
    new_data = []
    for index, row in df.iterrows():
        year = row['year']
        for word, count in row['fdist']:
            new_data.append([year, word, count])

    new_df = pd.DataFrame(new_data, columns=['year', 'word', 'count'])
   
    return new_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
