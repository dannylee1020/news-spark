import os
from dotenv import load_dotenv

load_dotenv()

NEWS_API_KEY = os.getenv('NEWS_API_KEY')
PROJECT_HOME = os.getenv('PROJECT_HOME')
CB_API_KEY = os.getenv('CB_API_KEY')
CB_API_SECRET = os.getenv('CB_API_SECRET')
CB_PW = os.getenv('CB_PW')