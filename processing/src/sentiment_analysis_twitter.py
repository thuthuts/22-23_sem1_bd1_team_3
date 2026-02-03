import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws
from pyspark.sql.types import StructType, StringType
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from dotenv import load_dotenv

load_dotenv()

# 4 underscores mean some value has to be inserted instead

# Setup Confluent Access Data
confluent_bootstrap_servers = os.getenv("BOOTSTRAP.SERVERS")
confluent_api_key = os.getenv("SASL.USERNAME")
confluent_password = os.getenv("SASL.PASSWORD")

# set environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Connection to MongoDB Cluster
connection_string = f"mongodb+srv://{os.getenv('MONGODB_USERNAME')}:{os.getenv('MONGODB_PASSWORD')}@{os.getenv('MONGODB_URI')}/"


# set spark conf
conf = SparkConf().set("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.6,"
                                              "org.mongodb.spark:mongo-spark-connector:10.0.5,"
                                              "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .set("spark.driver.memory","16G")\
    .set("spark.driver.maxResultSize", "0")\
    .set("spark.kryoserializer.buffer.max", "2000M")

sc = SparkContext(conf=conf)

my_spark = SparkSession.builder.appName("myApp") \
    .getOrCreate()
my_spark._jsc.hadoopConfiguration().set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")

# build pipeline for sentiment analysis
documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

tokenizer = Tokenizer() \
    .setInputCols(["document"]) \
    .setOutputCol("token")

# download pretrained model from spark-nlp
seq_classifier = BertForSequenceClassification.pretrained("bert_classifier_autotrain_financial_sentiment_765323474",
                                                          "en") \
    .setInputCols(["document", "token"]) \
    .setOutputCol("class")


pipeline = Pipeline(stages=[documentAssembler, tokenizer, seq_classifier])


# Reading Stream with Kafka
read_stream = my_spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', confluent_bootstrap_servers) \
    .option('kafka.security.protocol',  os.getenv('SECURITY.PROTOCOL')) \
    .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluent_api_key}" password="{confluent_password}";') \
    .option('kafka.ssl.endpoint.identification.algorithm', 'https') \
    .option('kafka.sasl.mechanism',  os.getenv("SASL.MECHANISM")) \
    .option('subscribe', 'twitter_news') \
    .option('failOnDataLoss', 'false') \
    .option('startingOffsets', 'earliest') \
    .load()


df = read_stream.selectExpr("CAST(value AS STRING)")

# load data to schema
schema = StructType()\
    .add('company', StringType()) \
    .add('tweet', StringType()) \
    .add("time", StringType())

df = df.select(from_json(col('value'), schema).alias('data'))
df = df.select('data.*')


df = df.select(col("company"), col("tweet"), col("time"))
tweets_df = df.toDF('company', "text", "time")

empty_df = my_spark.createDataFrame([['']]).toDF("text")

# Apply Pipeline, add headlines only as df
pipelineModel = pipeline.fit(empty_df)
result = pipelineModel.transform(tweets_df)

# Build final dataframe
df_results = result.select(col("company"), col("text"), col("time"), col("class.result"))
df_resultsFlatten = df_results.toDF("company", "tweet", "time", "class")


# Build Final Dataframe
df_results = df_resultsFlatten.withColumn("predicted", concat_ws("", col("class")))
df_results = df_results.drop("class")


# Writing Stream to MongoDB
df_results.writeStream \
    .format('mongodb') \
    .option('spark.mongodb.connection.uri', connection_string) \
    .option('spark.mongodb.database', 'News') \
    .option('spark.mongodb.collection', 'twitter_news_docker') \
    .option('checkpointLocation', "checkpoint3") \
    .outputMode('append') \
    .start() \
    .awaitTermination()
