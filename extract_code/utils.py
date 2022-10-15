import json
import pandas
from datetime import datetime
from logger import usecase_logger

logger = usecase_logger(__name__)


def line_mapper_to_record(json_data):
    """
    transforme json with dict python object format to a list of records of dict
    :param json_data:
    :return:
    """
    records = []
    for _, key_line in enumerate(json_data.keys()):
        records.append({'Line': key_line, **json_data[key_line]})
    return records


def transpose_df(dataframe=None):
    """
    transpose a dataframe
    :param dataframe:
    :return:
    """
    assert not dataframe.empty or dataframe is not None, 'dataframe should not be None or empty'
    logger.debug('transposing the dataframe')
    return dataframe.T


def list_records_to_df(list_records):
    """
    generate a dataframe by populating it with a list of records
    :param list_records:
    :return:
    """
    assert len(list_records) > 0, 'populate the dataframe with a none empty list'
    logger.debug('populate dataframe with data')
    return pandas.DataFrame(data=list_records)


def dqm(dataframe, nan_value='-', missing_value='-', columns_order=None, date_format='%Y-%m-%d'):
    """
    simple data quality management for processing nan and missing value from a dataframe,
    adding also a date of the processing and ordering the columns based on list passed
    :param dataframe:
    :param nan_value:
    :param missing_value:
    :param columns_order:
    :param date_format:
    :return:
    """
    assert not dataframe.empty or dataframe is not None, 'dataframe should not be None or empty'
    assert columns_order is not None, 'list of columns should not be None or empty'
    logger.debug('data quality processing')
    logger.debug('dataframe shape : {}'.format(dataframe.shape))
    dataframe = dataframe.fillna(value=nan_value).replace('', missing_value)
    dataframe['extraction_date'] = datetime.strftime(datetime.today(), date_format)
    if columns_order:
        df_columns = list(map(lambda column: column.lower(), dataframe.columns))
        dataframe.columns = df_columns
        dataframe = dataframe[columns_order]
    return dataframe


def read_jsonfile(filepath, mode='r'):
    """
    read json file and return a dict json object
    :param filepath:
    :param mode:
    :return:
    """
    logger.debug('reading {}'.format(filepath.split('/')[-1]))
    with open(filepath, mode=mode) as jsonfile:
        return json.load(jsonfile)


