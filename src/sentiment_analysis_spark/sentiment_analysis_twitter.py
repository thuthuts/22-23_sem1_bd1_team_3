from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType
import pyspark.sql.functions as F
from sparknlp.annotator import *
from sparknlp.base import *
from sparknlp.pretrained import PretrainedPipeline
from pyspark.ml import Pipeline
import sparknlp
import os
from dotenv import load_dotenv

load_dotenv()

# 4 underscores mean some value has to be inserted instead

# Run spark application
spark = sparknlp.start()


# Setup Confluent Access Data
confluentBootstrapServers = os.getenv('BOOTSTRAP.SERVERS')
confluentApiKey = os.getenv('SASL.USERNAME')
confluentSecret = os.getenv('SASL.PASSWORD')


# Connection to MongoDB Cluster
connectionString = f"mongodb+srv://{os.getenv('MONGODB.USERNAME')}:{os.getenv('MONGODB.PASSWORD')}@bdma.rvryhyj.mongodb.net/"


# Reading Stream with Kafka
read_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', confluent_bootstrap_servers) \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{confluent_api_key}" password="{confluent_password}";') \
    .option('kafka.ssl.endpoint.identification.algorithm', 'https') \
    .option('kafka.sasl.mechanism', 'PLAIN') \
    .option('subscribe', ____) \
    .option('failOnDataLoss', 'false') \
    .option('startingOffsets', 'earliest') \
    .load()

df = read_stream.selectExpr("CAST(value AS STRING)")


# Load data to schema
schema = StructType() \
    .add("id", StringType()) \
    .add("text", StringType()) \
    .add("author", StringType()) \
    .add("created", StringType()) \
    .add("timestamp", StringType()) \
    .add("company", StringType()) \
# The schema might be altered later

df = df.select(from_json(col('value'), schema).alias('data'))
df = df.select('data.*')
df.printSchema()

# df = df.select(col('text'), col('author'), col('created'), col('timestamp'), col('company'))
#
# df = df.toDF('text', 'author', 'created', 'timestamp', 'company')


# SparkNLP Model Pipeline
MODEL_NAME = 'sentimentdl_use_twitter'

documentAssembler = DocumentAssembler() \
    .setInputCol("text") \
    .setOutputCol("document")

use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en") \
    .setInputCols(["document"]) \
    .setOutputCol("sentence_embeddings")

sentimentdl = SentimentDLModel.pretrained(name=MODEL_NAME, lang="en") \
    .setInputCols(["sentence_embeddings"]) \
    .setOutputCol("sentiment")

nlpPipeline = Pipeline(
    stages=[
        documentAssembler,
        use,
        sentimentdl
    ])

# Apply Pipeline
empty_df = spark.createDataFrame([['']]).toDF('text')

pipelineModel = nlpPipeline.fit(empty_df)
result = pipelineModel.transform(df)
result.printSchema()


# Build Final Dataframe
# df_results = result.select(col('text'), col('author'), col('created'), col('timestamp'), col('company'), col('sentiment'))
# df_results = df_results.toDF(____)
# df_results = df_results.withColumn()


# Writing Stream to MongoDB
df_results.writeStream \
    .format('mongodb') \
    .option('spark.monogdb.connection.uri', connection_string) \
    .option('spark.mongodb.database', ____) \
    .option('spark.mongodb.collection', ____) \
    .option('checkpointLocation', ____) \
    .outputMode('append') \
    .start() \
    .awaitTermination()