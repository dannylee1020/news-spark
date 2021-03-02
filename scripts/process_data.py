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

# read price data
price_schema = StructType([
    StructField('time', LongType(), True),
    StructField('low', FloatType(), True),
    StructField('high', FloatType(), True),
    StructField('open', FloatType(), True),
    StructField('close', FloatType(), True),
    StructField('volume', FloatType(), True)
])

btc_price_txt = sc.textFile(f"{config.PROJECT_HOME}/raw_data/price/btc_price")
btc_price_data = btc_price_txt.collect()

# df = spark.createDataFrame(btc_price_data, schema=price_schema)
# btc_price_df = spark.read.format('csv').option('schema',price_schema).load(f"{config.PROJECT_HOME}/raw_data/price/btc_price")
# btc_price_txt = btc_price_txt.toDF()

# R = Row('time','low','high','open','close','volume')
# df = spark.createDataFrame(R(x) for x in btc_price_txt)


btc_price_txt = spark.read.text(f"{config.PROJECT_HOME}/raw_data/price/btc_price")


# df = spark.createDataFrame((x for x in btc_price_txt), schema = price_schema)

# eth_price_df = spark.read.load(f"{config.PROJECT_HOME}/raw_data/price/eth_price")






explode_articles_df = btc_news_df.withColumn('articles',explode('articles')).select('*', col('articles.*')).drop(col('articles'))
flatten_source_df = explode_articles_df.select('*', 'source.*').drop(*['source','status','totalResults'])




# perform transformations 





# write out to files
# load to Postgres


if __name__ == '__main__':
    # flatten_source_df.show(10)
    # btc_price_txt.printSchema()
    # temp_df.show(10)
    # df.show(10)
    # ast.literal_eval(btc_price_data)
    # df.show(10)

    

