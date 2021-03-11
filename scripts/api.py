import json, requests, hmac, hashlib, time, base64
from urllib.parse import urlencode
from requests.auth import AuthBase
from dotenv import load_dotenv
import os, config

class CoinbaseExchangeAuth(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or '')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message.encode(), hashlib.sha256)    
        signature_b64 = base64.b64encode(signature.digest())

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        })  
        return request


class Coinbase_API(CoinbaseExchangeAuth):
    def __init__(self, api_key, secret_key, passphrase):
        super().__init__(api_key, secret_key, passphrase)
        self.coinbaseAuth = CoinbaseExchangeAuth(self.api_key, self.secret_key, self.passphrase)    

    def get_historical_price(self, query = None, product_id = None, start = None, end = None, granularity = None):
        auth = self.coinbaseAuth
        endpoint = f"https://api.pro.coinbase.com/products/{product_id}/candles"
        if isinstance(query, dict):
            query_param = urlencode(query)
        else:
            query_param = urlencode({'start':start, 'end':end, 'granularity':granularity})

        lookup_url = f"{endpoint}?{query_param}"
        r = requests.get(lookup_url, auth=auth)

        return r.json()


class News_API(object):

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



class Bing_API(object):
    def __init__(self, api_key):
        self.api_key = api_key

    def get_headers(self):
        header = {
            'Ocp-Apim-Subscription-Key': self.api_key
        }

        return header

    def search_news(self, query, market= 'en-US', category=None, freshness= 'Week'):
        endpoint = 'https://api.bing.microsoft.com/v7.0/news/search'
        query_param = urlencode({
            'q':query,
            'mkt':market,
            'freshness':freshness
        })

        lookup_url = f"{endpoint}?{query_param}"
        r = requests.get(lookup_url, headers = self.get_headers())

        return r.json()



class Finnhub_API(object):
    def __init__(self, api_key):
        self.api_key = api_key
    

    def get_header(self):
        header = {
            'X-Finnhub-Token': self.api_key
        }

        return header

    def get_company_news(self, symbol, start_date, end_date):
        endpoint = 'https://finnhub.io/api/v1/company-news'
        query_param = urlencode({'symbol':symbol, 'from':start_date, 'to':end_date})
        lookup_url = f"{endpoint}?{query_param}"
        r = requests.get(lookup_url, headers = self.get_header())

        return r.json()

    def get_sec_filings(self, symbol=None, cik=None, accessNumber=None, form=None, start_date=None, end_date=None):
        endpoint = 'https://finnhub.io/api/v1'
        query_param = urlencode({
            'symbol':symbol,
            'cik':cik,
            'accessNumber':accessNumber,
            'form':form,
            'start_date':start_date,
            'end_date':end_date
        })
        lookup_url = f"{endpoint}?{lookup_url}"
        r = requests.get(lookup_url, headers = self.get_header())

        return r.json()
        

