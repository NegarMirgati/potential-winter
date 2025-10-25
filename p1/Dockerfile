FROM apache/spark-py:latest

USER root

RUN pip install --no-cache-dir delta-spark==2.4.0 boto3 pandas dotenv
RUN python3 -m pip install openpyxl

WORKDIR /p1

COPY . /p1

# Download required jars
RUN mkdir -p /opt/spark/jars && \
    wget -q -O /opt/spark/jars/delta-core_2.12-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    wget -q -O /opt/spark/jars/delta-storage-2.4.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar && \
    wget -q -O /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q -O /opt/spark/jars/aws-java-sdk-bundle-1.12.353.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.353/aws-java-sdk-bundle-1.12.353.jar && \
    chmod -R 644 /opt/spark/jars/*.jar


# Set environment variable for Spark to pick up jars
ENV SPARK_CLASSPATH=/opt/spark/jars/*
ENV SPARK_EXTRA_CLASSPATH=/opt/spark/jars/*

# export SPARK_JARS for pyspark to pick up
ENV SPARK_JARS=/opt/spark/jars/delta-core_2.12-2.4.0.jar,/opt/spark/jars/delta-storage-2.4.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.353.jar

CMD ["python3", "main.py", "data"]