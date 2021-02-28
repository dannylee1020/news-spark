import json, requests
from urllib.parse import urlencode
from requests.auth import AuthBase
from dotenv import load_dotenv
import os

class News_API(object):
    api_key = None

    def __init__(self, api_key):
        self.api_key = api_key
    
    def get_header(self):
        headers = {
            'Authorization' : f"Bearer {self.api_key}"
        }
        return headers
    
    def get_everything(self, query = None, query_title = None, start_date = None, end_date = None, sort_by = None):
        endpoint = 'https://newsapi.org/v2/everything'

        if isinstance(query, dict):
            # query = ' '.join([f"{k}:{v}" for k,v in query.items()])
            query_param = urlencode(query)
        else:
            query_param = urlencode({'q':query, 'qInTitle': query_title,'from':start_date, 'to':end_date, 'sortBy':sort_by})

        lookup_url = f"{endpoint}?{query_param}"
        r = requests.get(lookup_url, headers = self.get_header())

        return r.json()

    def get_top_headlines(self, query = None, country=None, category = None, sources=None):
        endpoint = 'https://newsapi.org/v2/top-headlines'
        query_param = urlencode({'q':query, 'country':country, 'category':category})
        lookup_url = f"{endpoint}?{query_param}"
        r = requests.get(lookup_url, headers = self.get_header())
        
        return r.json()



def write_json_file(path, data, filename):
    with open(path+filename, 'w', encoding='utf-8') as f:
        json.dump(data, f)


