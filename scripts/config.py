import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

NEWS_API_KEY = os.getenv('NEWS_API_KEY')
PROJECT_HOME = os.getenv('PROJECT_HOME')
CB_API_KEY = os.getenv('CB_API_KEY')
CB_API_SECRET = os.getenv('CB_API_SECRET')
CB_PW = os.getenv('CB_PW')
SPARK_HOME = os.getenv('SPARK_HOME')

# dynamically set date for api calls
START_DATE= (datetime.now() - timedelta(days = 27)).strftime('%Y-%m-%d')
END_DATE = datetime.now().strftime('%Y-%m-%d')
RAW_DATA_DIR = f"{PROJECT_HOME}/raw_data/"