import json
import pandas
from datetime import datetime
from logger import usecase_logger

logger = usecase_logger(__name__)


def line_mapper_to_record(json_data):
    records = []
    for _, key_line in enumerate(json_data.keys()):
        records.append({'Line': key_line, **json_data[key_line]})
    return records


def transpose_df(dataframe=None):
    assert not dataframe.empty or dataframe is not None, 'dataframe should not be None or empty'
    logger.debug('transposing the dataframe')
    return dataframe.T


def list_records_to_df(list_records):
    assert len(list_records) > 0, 'populate the dataframe with a none empty list'
    logger.debug('populate dataframe with data')
    return pandas.DataFrame(data=list_records)


def dqm(dataframe, nan_value='-', missing_value='-', columns_order=None, date_format='%Y-%m-%d'):
    assert not dataframe.empty or dataframe is not None, 'dataframe should not be None or empty'
    logger.debug('data quality processing')
    dataframe = dataframe.fillna(value=nan_value).replace('', missing_value)
    dataframe['extraction_date'] = datetime.strftime(datetime.today(), date_format)
    if columns_order:
        df_columns = list(map(lambda column: column.lower(), dataframe.columns))
        dataframe.columns = df_columns
        dataframe = dataframe[columns_order]
    return dataframe


def read_jsonfile(filepath, mode='r'):
    logger.debug('reading {}'.format(filepath.split('/')[-1]))
    with open(filepath, mode=mode) as jsonfile:
        return json.load(jsonfile)


