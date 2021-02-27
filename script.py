import json, hmac, hashlib, time, requests
from requests.auth import AuthBase
from dotenv import load_dotenv
import os

# authentication
load_dotenv()
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')

# Create custom authentication for Coinbase API
class CoinbaseWalletAuth(AuthBase):
    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key

    def __call__(self, request):
        timestamp = str(int(time.time()))
        message = timestamp + request.method + request.path_url + (request.body or '')
        signature = hmac.new(bytes(self.secret_key,encoding='ascii'), bytes(message, encoding = 'ascii'), hashlib.sha256).hexdigest()

        request.headers.update({
            'CB-ACCESS-SIGN': signature,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key
        })
        return request

api_url = 'https://api.coinbase.com/v2/'
auth = CoinbaseWalletAuth(API_KEY, API_SECRET)

r = requests.get(api_url+'accounts', auth=auth)



if __name__ == '__main__':
    print(r.json())
    # print(API_KEY)