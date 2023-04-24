from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3

# Create a SparkSession
spark = SparkSession.builder.appName("Transform twitter-data").getOrCreate()

# Read the CSV file from S3
s3_input_path_madrid = "s3://dexter-twitter-data-raw/madrid_tweets.csv"
madrid_df = spark.read.csv(s3_input_path_madrid, header=True, inferSchema=True)

s3_input_path_barca = "s3://dexter-twitter-data-raw/barca_tweets.csv"
barca_df = spark.read.csv(s3_input_path_barca, header=True, inferSchema=True)

# Perform transformations
teams_df = madrid_df.union(barca_df)

teams_df = teams_df.withColumn("tweet_date", date_format(col("created_at"), "yyyy-MM-dd"))

teams_df = teams_df.select('user', col('id').alias('tweet_id'), 'lang', 'source',
                  col('favorite_count').alias('likes'), col('retweet_count').alias('retweets'), 'tweet_date')

teams_df = teams_df.repartition(1)
# Write the transformed data back to S3
# teams_df.write.mode("overwrite").csv("s3://dexter-twitter-transformed-data/twitter_teams", header=True)
teams_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("s3://dexter-twitter-transformed-data/tweets")

s3 = boto3.resource('s3')

bucket = s3.Bucket('dexter-twitter-transformed-data')
objects = bucket.objects.filter(Prefix='tweets/')
csv_object = [o.key for o in objects if o.key.endswith('.csv')][0]

s3.Object('dexter-twitter-transformed-data', 'tweets/teams.csv').copy_from(
    CopySource=f'dexter-twitter-transformed-data/{csv_object}'
)
s3.Object('dexter-twitter-transformed-data', csv_object).delete()

# Stop the SparkSession
spark.stop()