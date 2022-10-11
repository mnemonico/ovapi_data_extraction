import logging
import requests
import pandas
from configuration import PER_LINE_ENDPOINT_REQUEST
from requests import HTTPError, RequestException, Timeout, ReadTimeout, URLRequired
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


@retry(retry=retry_if_exception_type(exception_types=(HTTPError, RequestException, Timeout, ReadTimeout, URLRequired)),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       stop=stop_after_attempt(100))
def get_lines():
    logger.debug('sending requesting')
    response = requests.get(url=PER_LINE_ENDPOINT_REQUEST)
    # content = response.content.decode('utf-8')
    content_json = response.json()
    logger.debug('response status : {code}'.format(code=response.status_code))
    return content_json


json_data = get_lines()

# pandas.DataFrame(data=get_lines())
