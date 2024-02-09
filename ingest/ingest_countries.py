from pyspark import SparkConf

import pyspark.sql.types as t
import pyspark.sql.functions as sf

from pyspark.sql.functions import col,current_timestamp

from pyspark.sql import SparkSession, DataFrame

import requests, json



url = "https://api.openaq.org/v2/countries?limit=100&page=1&offset=0&sort=asc&order_by=name"

headers = {
    "accept": "application/json",
    "content-type": "application/json"
}

response = requests.get(url, headers=headers)

p_json:json = json.loads(response.text)

#print (p_json.keys())
#exit()

"""
config = {
     "spark.jars.packages":"org.apache.hadoop:hadoop-aws:3.2.0,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3",
     "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
}
conf = SparkConf().setAll(config.items())

spark = SparkSession.builder.config(conf=conf).getOrCreate()
"""

spark = SparkSession.builder.getOrCreate()

#df = spark.read.json(p_json)


df = spark.read.json(spark.sparkContext.parallelize([p_json]))


df.printSchema()

df_results =df("results")
df_results.printSchema()

df_results.show()

