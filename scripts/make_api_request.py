from api import News_API, write_json_file, Coinbase_API, write_csv_file
from dotenv import load_dotenv
import os, json, config
from datetime import datetime, timedelta



news_api = News_API(config.NEWS_API_KEY)
cb_api = Coinbase_API(config.CB_API_KEY, config.CB_API_SECRET, config.CB_PW)

def create_query_param(news:bool, product, start_date, end_date, granularity='86400'):
    if news == True:
        query = {
            'q':product,
            'from':start_date,
            'to':end_date
        }
    else:
        query = {
            'start':start_date,
            'end':end_date,
            'granularity':granularity
        }
    
    return query


def make_api_request(api, product=None, product_id = None):
    if 'news' in str(type(api)).lower():
        query = create_query_param(True, product, config.START_DATE, config.END_DATE)
        data = api.get_everything(query = query)
    else:
        query = create_query_param(False, product_id, config.START_DATE, config.END_DATE)
        data = api.get_historical_price(product_id = product_id, query = query)
    
    return data


def write_json_file(path, data, filename):
    with open(path+filename, 'w', encoding='utf-8') as f:
        json.dump(data, f)



def write_csv_file(path, filename, data):
    with open(path+filename, 'w') as f:
        for line in data:
            f.write(f"{str(line).lstrip('[').rstrip(']')}\n")



# make requests
eth_news_data = make_api_request(news_api, product='ethereum')
btc_news_data = make_api_request(news_api, product = 'bitcoin')

eth_price_data = make_api_request(cb_api, product_id = 'ETH-USD')
btc_price_data = make_api_request(cb_api, product_id = 'BTC-USD')


# write to files
news_path = f"{config.PROJECT_HOME}/raw_data/news/"
price_path = f"{config.PROJECT_HOME}/raw_data/price/"
eth_news = 'eth_news.json'
btc_news = 'btc_news.json'
eth_price = 'eth_price.csv'
btc_price = 'btc_price.csv'


if __name__ == '__main__':
    write_json_file(path = news_path, data = eth_news_data, filename = eth_news)
    write_json_file(path = news_path, data = btc_news_data, filename = btc_news)
    write_csv_file(path = price_path, data = eth_price_data, filename = eth_price)
    write_csv_file(path = price_path, data = btc_price_data, filename = btc_price)
