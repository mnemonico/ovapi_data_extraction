import random
from tenacity import retry, wait_exponential, stop_after_attempt, wait_random_exponential, retry_if_exception_type, retry_if_result
from requests import HTTPError, RequestException, Timeout, ReadTimeout, URLRequired


@retry(retry=retry_if_exception_type(exception_types=IOError),
       wait=wait_exponential(multiplier=1, min=4, max=10),
       stop=stop_after_attempt(100))
def might_io_error():
    print("Retry forever with no wait if an IOError occurs, raise any other errors")
    raise IOError


might_io_error()
