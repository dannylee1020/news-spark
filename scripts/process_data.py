from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType
import config

spark = SparkSession.builder.master('local[*]').getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions', 5)

# read data
btc_df = spark.read.format('json').option('inferSchema','true').load(f"{config.PROJECT_HOME}/files/btc_data.json")
eth_df = spark.read.format('json').option('inferSchema','true').load(f"{config.PROJECT_HOME}/files/eth_data.json")

explode_articles_df = btc_df.withColumn('articles',explode('articles')).select('*', col('articles.*')).drop(col('articles'))
flatten_source_df = explode_articles.select('*', 'source.*').drop(col('source'))



if __name__ == '__main__':
    flatten_source_df.show(10)