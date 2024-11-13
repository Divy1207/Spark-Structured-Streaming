from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")
my_conf.set("spark.sql.shuffle.partitions", 3)

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

transaction_schema = "card_id long, amount long, postcode long, pos_id long, transaction_dt timestamp"

# creating the dataframe
transaction_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "9998") \
    .load()

value_df = transaction_df.select(from_json(col("value"), transaction_schema).alias("value"))

refined_transactions_df = value_df.select("value.*")

refined_transactions_df.printSchema()

member_df = spark.read \
.format("csv") \
.option("header", True) \
.option("inferSchema", True) \
.option("path", "E:/Big_data_ultimate/Week28/member_details.txt") \
.load()

join_expr = refined_transactions_df.card_id == member_df.card_id

join_type = "inner"

joined_df = refined_transactions_df.join(member_df, join_expr, join_type).drop(member_df["card_id"])

query = joined_df \
    .writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "checkpointlocation19") \
    .trigger(processingTime= "15 second") \
    .start()

query.awaitTermination()



