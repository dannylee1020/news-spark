from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, ArrayType
import config
import os



spark = SparkSession.builder.master('local[*]').getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions', 5)

# setting extraClassPath doesn't work in client mode.  
# .config('spark.driver.extraClassPath', f"{config.SPARK_HOME}/jars/postgresql-42.2.18.jar")\
# .config('spark.jars', f"{config.SPARK_HOME}/jars/postgresql-42.2.18.jar")\


# define schema for price data
price_schema = StructType([ 
    StructField('time', LongType(), True),
    StructField('low', FloatType(), True),  
    StructField('high', FloatType(), True),
    StructField('open', FloatType(), True),
    StructField('close', FloatType(), True),
    StructField('volume', FloatType(), True)
])


def spark_read_data(news:bool, path, filename, data_type):
    if news == True:
        new_path = f"{path}/news/"
        df = spark.read.format(data_type).option('inferSchema', 'true').load(new_path+filename)
    else:
        new_path = f"{path}/price/"
        df = spark.read.format(data_type).schema(price_schema).load(new_path)

    return df


file_path = config.RAW_DATA_DIR

# read news data
btc_news_df = spark_read_data(news=True, path=file_path, data_type='json', filename = 'btc_news.json')
eth_news_df = spark_read_data(news=True, path=file_path, data_type='json', filename = 'eth_news.json')

# flatten news_data
explode_articles_btc = btc_news_df.withColumn('articles',explode('articles')).select('*', col('articles.*')).drop(col('articles'))
flattened_btc_news = explode_articles_btc.select('*', 'source.*').drop(*['source','status','totalResults'])

explode_articles_eth = eth_news_df.withColumn('articles', explode('articles')).select('*', col('articles.*')).drop(col('articles'))
flattened_eth_news = explode_articles_eth.select('*', 'source.*').drop(*['source','status','totalResults'])

# add date column 
flattened_btc_news = flattened_btc_news.withColumn('published_date', to_date(to_timestamp(col('publishedAt')), 'yyyy-MM-dd'))
flattened_eth_news = flattened_eth_news.withColumn('published_date', to_date(to_timestamp(col('publishedAt')), 'yyyy-MM-dd'))



# read price data
btc_price_df = spark_read_data(news=False, path=file_path, data_type='csv', filename = 'btc_price.csv')
eth_price_df = spark_read_data(news=False, path=file_path, data_type='csv', filename = 'eth_price.csv')

# add date column
btc_df = btc_price_df.withColumn('date', to_date(to_timestamp(col('time')),'yyyy-MM-dd')).drop(col('time')) 
eth_df = eth_price_df.withColumn('date', to_date(to_timestamp(col('time')), 'yyyy-MM-dd')).drop(col('time'))


# Postgres connection
jdbc_url = 'jdbc:postgresql://localhost/news_spark'
properties = {
    'user':'dhyungseoklee', 
    'driver':'org.postgresql.Driver'    
}



if __name__ == '__main__':
    btc_df.write.jdbc(url=jdbc_url, table = 'btc_price', mode = 'append', properties = properties)
    eth_df.write.jdbc(url = jdbc_url, table = 'eth_price', mode = 'append', properties = properties)

    flattened_btc_news.write.jdbc(url = jdbc_url, table = 'btc_news', mode = 'append', properties = properties)
    flattened_eth_news.write.jdbc(url = jdbc_url, table = 'eth_news', mode = 'append', properties = properties)



    # spark-submit --driver-class-path /Users/dhyungseoklee/Projects/spark/spark-3.0.1-bin-hadoop2.7/jars/postgresql-42.2.18.jar process_data.py


    

