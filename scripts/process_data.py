from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
import config

spark = SparkSession.builder.master('local[*]').getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions', 5)

# read data
btc_df = spark.read.format('json').option('inferSchema','true').load(f"{config.PROJECT_HOME}/files/btc_data.json")
eth_df = spark.read.format('json').option('inferSchema','true').load(f"{config.PROJECT_HOME}/files/eth_data.json")


# elements_schema = StructType(
#     [
#         StructField('author', StringType, True),
#         StructField('content', StringType, True),
#         StructField('description', StringType, True),
#         StructField('publisedAt', StringType, True),
#         StructField('source', StructType([
#             StructField('id', StringType, True),
#             StructField('name', StringType, True)
#         ]),
#         StructField('title', StringtType, True),
#         StructField('url', StringType, True),
#         StructField('urlToImage', StringType, True)
#     ]
# )

# btc_elements_df = btc_df.select(col('articles.element.*'))
# eth_elements_df = eth_df.select(col('articles.element.*'))



if __name__ == '__main__':
    btc_df.printSchema()
    eth_df.printSchema()

    # btc_elements_df.printSchema()
    # eth_elements_df.printSchema()