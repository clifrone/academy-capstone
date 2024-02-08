from pyspark import SparkConf

import pyspark.sql.types as t
import pyspark.sql.functions as sf

from pyspark.sql.functions import col

from pyspark.sql import SparkSession, DataFrame

AWS_LOCATION="s3a://dataminded-academy-capstone-resources/raw/open_aq/"

SECRET_NAME="snowflake/capstone/config"


def read_json_aws(path):
    config = {
        "spark.jars.packages":"org.apache.hadoop:hadoop-aws:3.2.0,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }
    conf = SparkConf().setAll(config.items())

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark.read.json(path)

def retrieve_aws_secret(secretname):
    import botocore 
    import botocore.session 
    import json

    #from aws_secretsmanager_caching import SecretCache, SecretCacheConfig 

    client = botocore.session.get_session().create_client('secretsmanager')

    secret=client.get_secret_value(SecretId="snowflake/capstone/config")


#    import pdb
#    pdb.set_trace()

    json_secret=json.loads(secret["SecretString"])
    return(json_secret)
    
def clean_air_data(df):

    return df.select(
    col("city"),
    col("coordinates.latitude").alias("latitude"),
    col("coordinates.longitude").alias("longitude"),
    col("country"),
    col("date.local").alias("local_date"),
    col("date.utc").alias("utc_date"),
    col("entity"),
    col("isAnalysis"),
    col("isMobile"),
    col("location"),
    col("locationId"),
    col("parameter"),
    col("sensorType"),
    col("unit"),
    col("value")  
)

def store_data_in_snowflake(df:DataFrame):
    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"



    scrt=retrieve_aws_secret(SECRET_NAME)
 
    sfOptions = {
    "sfURL" : scrt["URL"],
    #"sfAccount" : scrt["DATABASE"],
    "sfUser" : scrt["USER_NAME"],
    "sfPassword" : scrt["PASSWORD"],
    "sfDatabase" : scrt["DATABASE"],
    "sfSchema" : "liviu",
    "sfWarehouse" : scrt["WAREHOUSE"]
    }

    (
        df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(**sfOptions)
        .option("dbtable", "raw_open_aq_data")
        .mode("Overwrite")
        .save()
    )

    return 0


df=read_json_aws(AWS_LOCATION)

clean_df=clean_air_data(df)

store_data_in_snowflake(clean_df)