from api import News_API, write_json_file
import config
from dotenv import load_dotenv
import os


news_api = News_API(config.API_KEY)

# set queries
eth_query = {
    'q':'ethereum',
    'from':'2021-01-28',
    'to': '2021-02-27'
}

btc_query = {
    'q':'bitcoin',
    'from':'2021-01-28',
    'to': '2021-02-27'
}

eth_data = news_api.get_everything(query=eth_query)
btc_data = news_api.get_everything(query = btc_query)

path = f"{config.PROJECT_HOME}/files/"
eth_filename = 'eth_data.json'
btc_filename = 'btc_data.json'


if __name__ == '__main__':
    write_json_file(path = path, data = eth_data, filename = eth_filename)
    write_json_file(path = path, data = btc_data, filename = btc_filename)