from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, to_date, desc
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



def perform_upsertion(url, table, properties):
    staging_table = "stg_"+table
    staging_data = spark.read.jdbc(url = url, table = staging_table, properties=properties)
    staging_data = staging_data.drop_duplicates()
    staging_data.write.jdbc(url=url, table=table, mode='overwrite', properties=properties)




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



# load data to staging table
staging_data = {
    'stg_btc_price':btc_df,
    'stg_eth_price':eth_df,
    'stg_btc_news':flattened_btc_news,
    'stg_eth_news':flattened_eth_news
}

for k,v in staging_data.items():
    v.write.jdbc(url=jdbc_url, mode='append', table=k, properties=properties)



if __name__ == '__main__':
    # load to prod table
    production_table = ['btc_price','eth_price','btc_news','eth_news']
    for name in production_table:
        perform_upsertion(jdbc_url, name, properties)
    


    # # spark-submit --driver-class-path /Users/dhyungseoklee/Projects/spark/spark-3.0.1-bin-hadoop2.7/jars/postgresql-42.2.18.jar process_data.py


    
