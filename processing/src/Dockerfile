# This Dockerfile is based on: 
# https://hub.docker.com/r/godatadriven/pyspark/dockerfile

# Selection of the (OS)Build
FROM openjdk:8-jre-slim

# Docker variable
ARG BUILD_DATE
ARG SPARK_VERSION=3.3.1

# Set labels (if you want to)
LABEL org.label-schema.name="Apache PySpark $SPARK_VERSION" \
      org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.version=$SPARK_VERSION

# Set environment variables in Docker Container
ENV PATH="/opt/miniconda3/bin:${PATH}"
ENV PYSPARK_PYTHON="/opt/miniconda3/bin/python"
ENV PYTHONUNBUFFERED=1

COPY sentiment_analysis_twitter.py sentiment_analysis_twitter.py
COPY sentiment_analysis_news_headlines.py sentiment_analysis_news_headlines.py
COPY sentiment_analysis_company_news.py sentiment_analysis_company_news.py

#WORKDIR /app


# copy the dependencies file to the working directory
COPY requirements.txt requirements.txt


# Install the required Applications
# iputils-ping is for testing purposes
RUN apt-get update && \
    apt-get install -y curl bzip2 --no-install-recommends && \
    apt-get install -y iputils-ping && \
    apt-get install -y apt-utils git curl wget ca-certificates bzip2 cmake tree htop bmon iotop g++  &&\
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
    bash /tmp/miniconda.sh -b -f -p "/opt/miniconda3" && \
    rm /tmp/miniconda.sh && \
    conda config --set auto_update_conda true && \
    conda config --set channel_priority false && \
    conda update conda -y --force && \
    conda clean -a && \
    conda install python=3.7 && \
    echo "PATH=/opt/miniconda3/bin:\${PATH}" > /etc/profile.d/miniconda.sh && \
    pip install --no-cache pyspark==${SPARK_VERSION} && \
    pip install -r requirements.txt && \
    SPARK_HOME=$(python /opt/miniconda3/bin/find_spark_home.py) && \
    echo "export SPARK_HOME=$(python /opt/miniconda3/bin/find_spark_home.py)" > /etc/profile.d/spark.sh && \
    wget "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.603/aws-java-sdk-1.11.603.jar" -O $SPARK_HOME/jars/aws-java-sdk-1.11.603.jar && \
    wget "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.0/hadoop-aws-3.3.0.jar" -O $SPARK_HOME/jars/hadoop-aws-3.3.0.jar && \
    mkdir -p $SPARK_HOME/conf && \
    echo "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" >> $SPARK_HOME/conf/spark-defaults.conf && \
    apt-get remove -y curl bzip2 && \
    apt-get autoremove -y && \
    apt-get clean
