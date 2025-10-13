FROM apache/spark:3.5.1-python3

USER root

RUN apt-get update \
    && apt-get install -y wget \
    && rm -rf /var/lib/apt/lists/*

RUN wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.76.6/nessie-spark-extensions-3.5_2.12-0.76.6.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
    
    # https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.132/bundle-2.20.132.jar \
    # https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.132/url-connection-client-2.20.132.jar


ENV SPARK_CLASSPATH="/opt/spark/jars/*"

COPY ./requirements.txt /
RUN pip install -r /requirements.txt

# USER spark
