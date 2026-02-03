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
    .set("spark.executor.memory", "16G")\
    .set("spark.driver.maxResultSize", "0")\
    .set("spark.kryoserializer.buffer.max", "2000M")\

sc = SparkContext(conf=conf)

my_spark = SparkSession.builder.appName("myApp") \
    .getOrCreate()

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

# seq_classifier = BertEmbeddings.load('tmp/bert_classifier_autotrain_financial_sentiment')\
#     .setInputCols(["document", 'token'])\
#     .setOutputCol("class")\
#     .setCaseSensitive(False)\
#     # .setPoolingLayer(0)

pipeline = Pipeline(stages=[documentAssembler, tokenizer, seq_classifier])


# Reading Stream with Kafka
read_stream = my_spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', confluent_bootstrap_servers) \
    .option('kafka.security.protocol', os.getenv('SECURITY.PROTOCOL')) \
    .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluent_api_key}" password="{confluent_password}";') \
    .option('kafka.ssl.endpoint.identification.algorithm', 'https') \
    .option('kafka.sasl.mechanism',  os.getenv("SASL.MECHANISM")) \
    .option('subscribe', 'nasdaq_news') \
    .option('failOnDataLoss', 'false') \
    .option('startingOffsets', 'earliest') \
    .load()


df = read_stream.selectExpr("CAST(value AS STRING)")

# Load data into schema
schema = (
        StructType()
        .add("news", StructType()
             .add("headline", StringType())
             .add("description", StringType())
             .add("time", StringType())
             .add("more_info", StringType()))
        .add("time", StringType())
    )

news_df = df.select(from_json(col('value'), schema).alias('data'))
headlines_df = news_df.select('data.*')


df = headlines_df.select(col("news.headline"), col("news.description"), col("news.more_info"), col("news.time"),col("time"))
headlines_df = df.toDF('text', "description","more_info", "time", "timestamp")

empty_df = my_spark.createDataFrame([['']]).toDF("text")


# Apply Pipeline, add headlines only as df
pipelineModel = pipeline.fit(empty_df)
result = pipelineModel.transform(headlines_df)

# Build final dataframe
df_results = result.select(col("text"), col("description"), col("more_info"), col("time"), col("timestamp"), col("class.result"))
df_resultsFlatten = df_results.toDF("headline", "description", "more_info", "time", "timestamp", "class")


#df_resultsFlatten = df_results.toDF("text", "class")

finished_df = df_resultsFlatten.withColumn('class', F.explode('class'))

# Writing Stream to MongoDB
finished_df.writeStream \
    .format('mongodb') \
    .option('spark.mongodb.connection.uri', connection_string) \
    .option('spark.mongodb.database', 'News') \
    .option('spark.mongodb.collection', 'ML_news') \
    .option('checkpointLocation', "checkpoint") \
    .outputMode('append') \
    .start() \
    .awaitTermination()
