import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws
from pyspark.sql.types import StructType, StringType
import pyspark.sql.functions as F
from dotenv import load_dotenv

load_dotenv()


# 4 underscores mean some value has to be inserted instead

# Setup Confluent Access Data
confluentBootstrapServers = os.getenv('BOOTSTRAP.SERVERS')
confluentApiKey = os.getenv('SASL.USERNAME')
confluentSecret = os.getenv('SASL.PASSWORD')

# set environment variables
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Connection to MongoDB Cluster
connectionString = f"mongodb+srv://{os.getenv('MONGODB.USERNAME')}:{os.getenv('MONGODB.PASSWORD')}@bdma.rvryhyj.mongodb.net/"


spark = sparknlp.start()
my_spark = SparkSession \
    .builder \
    .appName("testApp") \
    .config("spark.driver.memory","16G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.2.2,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")\
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

pipeline = Pipeline(stages=[documentAssembler, tokenizer, seq_classifier])

# Reading Stream with Kafka
read_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', confluent_bootstrap_servers) \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluent_api_key}" password="{confluent_password}";') \
    .option('kafka.ssl.endpoint.identification.algorithm', 'https') \
    .option('kafka.sasl.mechanism', 'PLAIN') \
    .option('subscribe', "nasdaq_news") \
    .option('failOnDataLoss', 'false') \
    .option('startingOffsets', 'earliest') \
    .load()

df = read_stream.selectExpr("CAST(value AS STRING)")

# load data to schema
schema = StructType()\
    .add('sentiment', StringType(), True) \
    .add('headline', StringType(), True)

df = df.select(from_json(col('value'), schema).alias('data'))
df = df.select('data.*')

df.printSchema()

df = df.select(['headline'])

# rename headline column to 'text' to fit to pipeline
headlines_df = df.toDF('text')

# what is this line doing?
empty_df = spark.createDataFrame([['']]).toDF('text')

# Apply Pipeline, add headlines only as df
pipelineModel = pipeline.fit(empty_df)
pipelineModel = pipeline.fit(headlines_df)
result = pipelineModel.transform(headlines_df)

# Build Final Dataframe
df_results = result.select("text", "class.result")
df_results = df_results.toDF("text", "predicted")
df_results = df_results.withColumn("predicted2", concat_ws("", col("predicted")))
df_results = df_results.drop("predicted")

# data frame now contains columns "Text" with the headline and "predicted" with the sentiment
# (negative, neutral or positive)
df_results = df_results.withColumnRenamed("predicted2", "predicted")

# Writing Stream to MongoDB
df_results.writeStream \
    .format('mongodb') \
    .option('spark.monogdb.connection.uri', connection_string) \
    .option('spark.mongodb.database', "News") \
    .option('spark.mongodb.collection', "ML_news") \
    .option('checkpointLocation', "gs://firstsparktest_1/checkpointsentimentcompanynews") \
    .outputMode('append') \
    .start() \
    .awaitTermination()
