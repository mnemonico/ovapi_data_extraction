import logging
import requests
import pandas
from utils import line_mapper_to_record, list_records_to_df, dqm
from configuration import PER_LINE_ENDPOINT_REQUEST, MAX_ATTEPTS, WAIT_EXPONENTIAL_MAX, WAIT_EXPONENTIAL_MULTIPLAYER, WAIT_EXPONENTIAL_MIN, NAN_VALUE, MISSING_VALUE
from requests import HTTPError, RequestException, Timeout, ReadTimeout, URLRequired
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


@retry(retry=retry_if_exception_type(exception_types=(RequestException, Timeout, ReadTimeout)),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       stop=stop_after_attempt(100))
def get_lines():
    logger.debug('sending requesting')
    response = requests.get(url=PER_LINE_ENDPOINT_REQUEST)
    content_json = response.json()
    logger.debug('response status : {code}'.format(code=response.status_code))
    return content_json


records = line_mapper_to_record(json_data=get_lines())
df = list_records_to_df(list_records=records)
df_dqm = dqm(dataframe=df, nan_value=NAN_VALUE, missing_value=MISSING_VALUE)
print(df_dqm.head(10))
