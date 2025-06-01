FROM flink:1.18

# Add python symlink first
RUN ln -s /usr/bin/python3 /usr/bin/python

# Then install dependencies
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies first
COPY requirements.txt /opt/requirements.txt
RUN pip3 install -r /opt/requirements.txt && \
    pip3 install --force-reinstall numpy==1.26.4

# Then download Kafka connectors
RUN wget -P /opt/flink/lib/ \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.2.0-1.18/flink-connector-kafka-3.2.0-1.18.jar \
    https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.2/clickhouse-jdbc-0.7.2.jar

ENV PYFLINK_PYTHON=python3
ENV PYFLINK_CLIENT_EXECUTABLE=python3