from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, ArrayType
import config

spark = SparkSession.builder.master('local[*]').getOrCreate()
sc = spark.sparkContext
spark.conf.set('spark.sql.shuffle.partitions', 5)

# read news data
btc_news_df = spark.read.format('json').option('inferSchema','true').load(f"{config.PROJECT_HOME}/raw_data/news/btc_news.json")
eth_news_df = spark.read.format('json').option('inferSchema','true').load(f"{config.PROJECT_HOME}/raw_data/news/eth_news.json")

explode_articles_df = btc_news_df.withColumn('articles',explode('articles')).select('*', col('articles.*')).drop(col('articles'))
flatten_source_df = explode_articles_df.select('*', 'source.*').drop(*['source','status','totalResults'])

# read price data
price_schema = StructType([
    StructField('time', LongType(), True),
    StructField('low', FloatType(), True),
    StructField('high', FloatType(), True),
    StructField('open', FloatType(), True),
    StructField('close', FloatType(), True),
    StructField('volume', FloatType(), True)
])

btc_price_df = spark.read.format('csv').schema(price_schema).load(f"{config.PROJECT_HOME}/raw_data/price/btc_price.csv")
eth_price_df = spark.read.format('csv').schema(price_schema).load(f"{config.PROJECT_HOME}/raw_data/price/eth_price.csv")






# perform transformations 





# write out to files
# load to Postgres


if __name__ == '__main__':
    # flatten_source_df.show(10)
    btc_price_df.show(10)

    

