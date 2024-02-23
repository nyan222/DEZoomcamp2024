import snscrape.modules.instagram as sntig
import pandas as pd
import re
import datetime 

# Creating list to append tweet data near:"Turin" within:10km
pst_list = []
cutoffDate = datetime.date(2023, 10, 1)
startDate = datetime.date(2023, 9, 1)

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    for i, pst in enumerate(sntig.InstagramHashtagScraper('romanempire').get_items()):  # declare a username
        if pst.date.date() <= cutoffDate or pst.date.date() >= startDate or pst.likes <50:
	        break
        # if i > 10:  # number of posts you want to scrape
        #     break
        pst_list.append([id,item.date,item.cleanUrl, item.dirtyUrl, item.content, item.thumbnailUrl, item.displayUrl, item.username ,item.likes, item.comments])  # declare the attributes to be returned

    # Creating a dataframe from the tweets list above
    pst_df = pd.DataFrame(pst_list,columns=['ID','Date','Url', 'dirtyUrl', 'Text', 'thumbnailUrl','displayUrl' ,'username',  'likes', 'Comments'])
    pst_df2 = pst_df['Device'].apply(lambda s: re.sub('<[^\>]*>', '', s))

    pst_df = pst_df.drop(['Device'], axis=1)
    pst_df = pst_df.merge(pst_df2, left_index=True, right_index=True)

    return {pst_df}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
