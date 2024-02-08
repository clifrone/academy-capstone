from pyspark import SparkConf

import pyspark.sql.types as t
import pyspark.sql.functions as sf

from pyspark.sql import SparkSession, DataFrame

AWS_LOCATION="s3a://dataminded-academy-capstone-resources/raw/open_aq/"


def read_json_aws(path):


    config = {
        "spark.jars.packages":"org.apache.hadoop:hadoop-aws:3.2.0",
        #"spark.hadoop.fs.s3a.aws.credentials.provider":"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }
    conf = SparkConf().setAll(config.items())

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark.read.json(path)
#,
        # For a CSV, `inferSchema=False` means every column stays of the string
        # type. There is no time wasted on inferring the schema, which is
        # arguably not something you would depend on in production either.
        #inferSchema=False,
        #header=True,
        # The dataset mixes two values for null: sometimes there's an empty attribute,
        # which you will see in the CSV file as two neighboring commas. But there are
        # also literal "null" strings, like in this sample: `420.0,null,,1.0,`
        # The following option makes a literal null-string equivalent to the empty value.
      #  nullValue="null",
    
def clean_air_data(df)(

df = df.select(
  col("city"),
  col("coordinates.latitude").alias("latitude"),
  col("coordinates.longitude").alias("longitude"),
  col("country"),
  col("date.local").alias("local_date"),
  col("date.utc").alias("utc_date")
  col("entity").alias("entity")
  col("isAnalysis").alias("isAnalysis")
  col("date.utc").alias("utc_date")
  col("date.utc").alias("utc_date")
  col("date.utc").alias("utc_date")
  col("date.utc").alias("utc_date")
  col("date.utc").alias("utc_date")
  col("date.utc").alias("utc_date")
  
)



)

df=read_json_aws(AWS_LOCATION)

df.printSchema()