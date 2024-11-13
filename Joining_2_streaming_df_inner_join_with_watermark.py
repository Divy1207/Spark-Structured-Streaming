from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")
my_conf.set("spark.sql.shuffle.partitions", 3)

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

# Define the schema for impressions stream
impressions_schema = StructType([
    StructField("impressionID", StringType(), True),
    StructField("ImpressionTime", TimestampType(), True),
    StructField("CampaignName", StringType(), True)
])

# Define the schema for clicks stream
clicks_schema = StructType([
    StructField("clickID", StringType(), True),
    StructField("ClickTime", TimestampType(), True)
])

# creating the dataframe impression_df
impression_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", "9998") \
    .load()


# creating the dataframe click_df
click_df = spark \
.readStream \
.format("socket") \
.option("host", "localhost") \
.option("port", "9999") \
.load()

# Structure the data based on the schema defined - impressionDf
valueDF1 = impression_df.select(from_json(col("value"), impressions_schema).alias("value"))
impressionDfNew = valueDF1.select("value.*").withWatermark("ImpressionTime", "30 minute")

# Structure the data based on the schema defined - clickDf
valueDF2 = click_df.select(from_json(col("value"), clicks_schema).alias("value"))
clickDfNew = valueDF2.select("value.*").withWatermark("clickTime", "30 minute")

# Join condition
joinExpr = impressionDfNew.impressionID == clickDfNew.clickID

# Join type
joinType = "inner"

# Joining both the streaming data frames
joinedDf = impressionDfNew.join(clickDfNew, joinExpr, joinType).drop(clickDfNew["clickID"])

# Output to the sink
campaignQuery = joinedDf.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "chk-Loc12") \
    .trigger(processingTime="15 seconds") \
    .start()

campaignQuery.awaitTermination()


