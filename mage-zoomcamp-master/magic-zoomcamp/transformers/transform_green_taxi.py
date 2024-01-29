import re

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    print(f"Preprocessing rows with zero passengers: { data[['passenger_count']].isin([0]).sum() }")
    print(f"Preprocessing rows with zero trip_distance: { data[['trip_distance']].isin([0]).sum() }")
    print(f"Preprocessing rows with null trip_distance: { data[['trip_distance']].isna().sum() }")
    
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    pattern = re.compile("([A-Z]+[a-z]+)([A-Z]+)")  # Replace "your_pattern_here" with your regular expression pattern
    matching_columns = [col for col in data.columns if pattern.match(col)]
    num_matching_columns = len(matching_columns)
    print(f"Fields to change into snakecase: {num_matching_columns}")

    data.columns = (data.columns
                .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                .str.lower()
             )

    return data[(data['passenger_count']>0) & (data['trip_distance']>0)]


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with zero trip distance'
    assert 'vendor_id' in output, 'There is no column "vendor_id"'