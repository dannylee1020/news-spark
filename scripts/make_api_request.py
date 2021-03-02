from api import News_API, write_json_file, Coinbase_API, write_csv_file
import config
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta


news_api = News_API(config.NEWS_API_KEY)
cb_api = Coinbase_API(config.CB_API_KEY, config.CB_API_SECRET, config.CB_PW)

# dynamically set date
start_date= (datetime.now() - timedelta(days = 27)).strftime('%Y-%m-%d')
end_date = datetime.now().strftime('%Y-%m-%d')

# News data
eth_query = {
    'q':'ethereum',
    'from':start_date,
    'to': end_date
}

btc_query = {
    'q':'bitcoin',
    'from':start_date,
    'to': end_date
}

eth_news_data = news_api.get_everything(query=eth_query)
btc_news_data = news_api.get_everything(query = btc_query)


# price data
btc_q = {
    'start':start_date,
    'end' : end_date,
    'granularity':'86400'
}

eth_q = {
    'start':start_date,
    'end' : end_date,
    'granularity':'86400'
}

btc_price_data = cb_api.get_historical_price(product_id = 'BTC-USD', query = btc_q)
eth_price_data = cb_api.get_historical_price(product_id = 'ETH-USD', query = eth_q)


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

