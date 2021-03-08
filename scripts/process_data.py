from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, LongType, FloatType, ArrayType
import config, os, psycopg2
import pandas as pd


master = f"spark://{config.LOCALHOST}:7077"
spark = SparkSession.builder.master(master).getOrCreate()

# Postgres connection
jdbc_url = 'jdbc:postgresql://localhost/news_spark'
properties = {
    'user':'dhyungseoklee', 
    'driver':'org.postgresql.Driver'    
}

# #control resources for running the applications here if needed
# spark = SparkSession.builder.master(master)\
#     .config('spark.sql.shuffle.partitions','5')\
#     .config('spark.executor.memory','5g')\
#     .config('spark.executor.cores', '3')\
#     .config('spark.executor.instances', '3')\
#     .getOrCreate() 


def spark_read_data(news:bool, path, filename, data_type):
    if news == True:
        new_path = f"{path}/news/"
        df = spark.read.format(data_type).option('inferSchema', 'true').load(new_path+filename)
    else:
        new_path = f"{path}/price/"
        df = spark.read.format(data_type).schema(price_schema).load(new_path+filename)

    return df


def db_table_exists(tablename):
    conn = psycopg2.connect(user='dhyungseoklee', database='news_spark', host='localhost')
    cur = conn.cursor()
    cur.execute(f"select exists(select * from information_schema.tables where table_name='{tablename}')")
    result = cur.fetchone()[0]
    cur.close()
    conn.close()

    return result


def dataframe_upsertion(tablename, url, new_data):
    if db_table_exists(tablename):
        df_from_db = spark.read.jdbc(url=url, table=tablename, properties=properties)
        new_df = df_from_db.union(new_data)
        upserted_df = new_df.drop_duplicates()
    else:
        upserted_df = new_data
    
    return upserted_df




# define schema for price data
price_schema = StructType([ 
    StructField('time', LongType(), True),
    StructField('low', FloatType(), True),  
    StructField('high', FloatType(), True),
    StructField('open', FloatType(), True),
    StructField('close', FloatType(), True),
    StructField('volume', FloatType(), True)
])


# read news data
btc_news_df = spark_read_data(news=True, path=config.RAW_DATA_DIR, data_type='json', filename = 'btc_news.json')
eth_news_df = spark_read_data(news=True, path=config.RAW_DATA_DIR, data_type='json', filename = 'eth_news.json')

# flatten news_data
explode_articles_btc = btc_news_df.withColumn('articles',explode('articles')).select('*', col('articles.*')).drop(col('articles'))
flattened_btc_news = explode_articles_btc.select('*', 'source.*').drop(*['source','status','totalResults', 'urlToImage'])

explode_articles_eth = eth_news_df.withColumn('articles', explode('articles')).select('*', col('articles.*')).drop(col('articles'))
flattened_eth_news = explode_articles_eth.select('*', 'source.*').drop(*['source','status','totalResults', 'urlToImage'])

# add date column 
flattened_btc_news = flattened_btc_news.withColumn('published_date', to_date(to_timestamp(col('publishedAt')), 'yyyy-MM-dd'))
flattened_eth_news = flattened_eth_news.withColumn('published_date', to_date(to_timestamp(col('publishedAt')), 'yyyy-MM-dd'))



# read price data
btc_price_df = spark_read_data(news=False, path=config.RAW_DATA_DIR, data_type='csv', filename = 'btc_price.csv')
eth_price_df = spark_read_data(news=False, path=config.RAW_DATA_DIR, data_type='csv', filename = 'eth_price.csv')

# add date column
btc_df = btc_price_df.withColumn('date', to_date(to_timestamp(col('time')),'yyyy-MM-dd')).drop(col('time')) 
eth_df = eth_price_df.withColumn('date', to_date(to_timestamp(col('time')), 'yyyy-MM-dd')).drop(col('time'))



# perform manual upsertion on DF
btc_price = dataframe_upsertion('btc_price', jdbc_url, btc_df)
eth_price = dataframe_upsertion('eth_price', jdbc_url, eth_df)
btc_news = dataframe_upsertion('btc_news', jdbc_url, flattened_btc_news)
eth_news = dataframe_upsertion('eth_news', jdbc_url, flattened_eth_news)



if __name__ == '__main__':
    btc_price.write.jdbc(url=jdbc_url, table = 'btc_price', mode = 'overwrite', properties = properties)
    eth_price.write.jdbc(url = jdbc_url, table = 'eth_price', mode = 'overwrite', properties = properties)
    btc_news.write.jdbc(url = jdbc_url, table = 'btc_news', mode = 'overwrite', properties = properties)
    eth_news.write.jdbc(url = jdbc_url, table = 'eth_news', mode = 'overwrite', properties = properties)



    # spark-submit --driver-class-path /Users/dhyungseoklee/Projects/spark/spark-3.0.1-bin-hadoop2.7/jars/postgresql-42.2.18.jar process_data.py


    
