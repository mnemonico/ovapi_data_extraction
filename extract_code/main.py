import requests
from logger import usecase_logger
from orm import db_engine, create_table, get_table_object, stmt_insert_update
from utils import line_mapper_to_record, list_records_to_df, dqm, read_jsonfile
from configuration import PER_LINE_ENDPOINT_REQUEST, MAX_ATTEPTS, WAIT_EXPONENTIAL_MAX, WAIT_EXPONENTIAL_MULTIPLAYER, WAIT_EXPONENTIAL_MIN, NAN_VALUE, MISSING_VALUE
from requests import HTTPError, RequestException, Timeout, ReadTimeout, URLRequired
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from dialect_datacatalog import line_table


logger = usecase_logger(__name__)


@retry(retry=retry_if_exception_type(exception_types=(RequestException, Timeout, ReadTimeout)),
       wait=wait_exponential(multiplier=WAIT_EXPONENTIAL_MULTIPLAYER, min=WAIT_EXPONENTIAL_MIN, max=WAIT_EXPONENTIAL_MAX),
       stop=stop_after_attempt(MAX_ATTEPTS))
def get_lines():
    """
    this function take as decorator retry behavior for processing the api call without having request typical error of
    limitation, pool connection, timeouts, ...
    :return:
    """
    logger.debug('sending requesting to api')
    response = requests.get(url=PER_LINE_ENDPOINT_REQUEST)
    content_json = response.json()
    logger.debug('response status : {code}'.format(code=response.status_code))
    return content_json


def sql_ddl(tablename=None):
    create_table(*line_table['ddl'],
                 tablename='ods_{feed}'.format(feed=tablename),
                 engine=db_engine)


def retreive_table(tablename=None):
    return get_table_object(tablename=tablename, engine=db_engine)


def main():
    logger.debug('start extraction')
    # calling api /line endpoint to fetch lines data
    json_data = get_lines()
    # transform json api response to list of dict record
    records = line_mapper_to_record(json_data=json_data)

    # generate dataframe from list of records
    df = list_records_to_df(list_records=records)
    # simple data quality function to process nan, empty, columns order
    df_dqm = dqm(dataframe=df, nan_value=NAN_VALUE, missing_value=MISSING_VALUE, columns_order=line_table['columns'])
    # transform dataframe to list of dict record
    data_records = df_dqm.to_dict(orient='records')

    # create table programmatically
    sql_ddl(tablename=line_table['tablename'])
    # get table orm object to interact with it in class method way
    line = retreive_table(tablename='line')
    # sql upsert with specific postgres dialects
    stmt_insert_update(table=line, records_to_insert=data_records)
    logger.debug('extraction finished')


if __name__ == '__main__':
    main()
