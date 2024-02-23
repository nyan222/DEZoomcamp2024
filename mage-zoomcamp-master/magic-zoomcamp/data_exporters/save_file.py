from mage_ai.io.file import FileIO
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_file(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to filesystem.

    Docs: https://docs.mage.ai/design/data-loading#fileio
    """
    t = kwargs.get('var22')
    file_name = f"roman_news_{t}.parquet"
    df.to_parquet(file_name, engine='pyarrow',coerce_timestamps="ms",allow_truncated_timestamps=True)#
    filepath = f"/home/src/files/{file_name}"
    FileIO().export(df, filepath)
