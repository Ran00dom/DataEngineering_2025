import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

class RequestsController:

    def __init__(self, url):
        self.url = url
        self.session = requests.Session()
        self.retry = Retry(connect=3, backoff_factor=0.5)
        self.adapter = HTTPAdapter(max_retries=self.retry)
        self.session.mount('http://', self.adapter)
        self.session.mount('https://', self.adapter)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def setUrl(self, url):
        self.url = url

    def getRequestJSON(self):
        try:
            return self.session.get(self.url).json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f'Request Exception: {format(e)}')
            return None
        except Exception as e:
            self.logger.error(f'Unexpected error: {format(e)}')
            return None
        except ValueError as e:
            self.logger.error(f'Invalid JSON response: {format(e)}')
            return None

    def __del__(self):
        self.session.close()