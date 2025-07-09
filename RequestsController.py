import json

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from json import loads

class RequestsController:

    def __init__(self, url):
        self.url = url
        self.session = requests.Session()
        self.retry = Retry(connect=3, backoff_factor=0.5)
        self.adapter = HTTPAdapter(max_retries=self.retry)
        self.session.mount('http://', self.adapter)
        self.session.mount('https://', self.adapter)
        self.text = json.load(open('C:\InProgres\DataEngineering_2025\sest.json'))

    def setUrl(self, url):
        self.url = url

    def getRequestJSON(self):
        #return self.text
        return self.session.get(self.url).json()
