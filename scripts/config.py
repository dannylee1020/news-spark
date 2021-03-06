import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

NEWS_API_KEY = os.getenv('NEWS_API_KEY')
PROJECT_HOME = os.getenv('PROJECT_HOME')
CB_API_KEY = os.getenv('CB_API_KEY')
CB_API_SECRET = os.getenv('CB_API_SECRET')
CB_PW = os.getenv('CB_PW')
FINNHUB_API_KEY = os.getenv('FINNHUB_API_KEY')
FINNHUB_SANDBOX = os.getenv('FINNHUB_SANDBOX')
CRYPTOPANIC_API_KEY = os.getenv('CRYPTOPANIC_API_KEY')
AZURE_SUBSCRIPTION_KEY = os.getenv('AZURE_SUBSCRIPTION_KEY')
LOCALHOST = os.getenv('IP_ADDRESS')

# dynamically set date for api calls
START_DATE= (datetime.now() - timedelta(days = 27)).strftime('%Y-%m-%d')
END_DATE = datetime.now().strftime('%Y-%m-%d')
RAW_DATA_DIR = f"{PROJECT_HOME}/raw_data/"