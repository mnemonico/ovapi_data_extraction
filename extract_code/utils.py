import pandas
import json


def line_mapper_to_record(json_data):
    records = []
    for _, key_line in enumerate(json_data.keys()):
        records.append({'Line': key_line, **json_data[key_line]})
    return records


def transpose_df(dataframe=None):
    return dataframe.T


def list_records_to_df(list_records):
    return pandas.DataFrame(data=list_records)


def dqm(dataframe, nan_value='-', missing_value='-'):
    return dataframe.fillna(value=nan_value).replace('', missing_value)
