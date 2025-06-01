
## nanti buat superset nya simpen data ke postgres







































kafka-connect:
    image: confluentinc/cp-kafka-connect:7.3.0
    container_name: kafka-connect
    depends_on:
      kafka:
        condition: service_healthy
      minio:
        condition: service_healthy
    ports:
      - "8084:8083"
    environment:
      CONNECT_BOOTSTRAP_SERVERS: kafka:9092
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
      CONNECT_REST_PORT: 8083
      CONNECT_GROUP_ID: s3-sink-group
      CONNECT_CONFIG_STORAGE_TOPIC: s3-sink-configs
      CONNECT_OFFSET_STORAGE_TOPIC: s3-sink-offsets
      CONNECT_STATUS_STORAGE_TOPIC: s3-sink-status
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 1
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE: false
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: false
      CONNECT_PLUGIN_PATH: "/usr/share/java,/usr/share/confluent-hub-components,/etc/kafka-connect/jars"
    command:
      - bash
      - -c
      - |
        echo "Installing S3 Sink Connector..."
        confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.0
        confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:7.3.3
        echo "Starting Kafka Connect..."
        /etc/confluent/docker/run
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8083/connectors"]
      interval: 30s
      timeout: 10s
      retries: 10
      start_period: 60s
    networks:
      - streaming-network
    restart: unless-stopped


* set S3 Sink connector (outside container)
curl -X POST http://localhost:8084/connectors -H "Content-Type: application/json" -d '{
  "name": "minio-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "flush.size": "3",
    "schema.compatibility": "NONE",
    "topics": "source.public.logistics",
    "tasks.max": "1",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "s3.bucket.name": "datalake",
    "s3.endpoint": "http://minio:9001",
    "s3.region": "us-east-1",
    "s3.ssl.enabled": "false",
    "s3.path.style.access": "true",
    "s3.access.key": "minioadmin",
    "s3.secret.key": "minioadmin",
    "aws.access.key.id": "minioadmin",
    "aws.secret.access.key": "minioadmin",
    "store.url": "http://minio:9001",
    "s3.credentials.provider.class": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "true",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "enhanced.avro.schema.support": "true"
  }
}'


* set S3 Sink connector (inside container)
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d '{
  "name": "minio-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "flush.size": "3",
    "schema.compatibility": "NONE",
    "topics": "source.public.logistics",
    "tasks.max": "1",
    "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
    "storage.class": "io.confluent.connect.s3.storage.S3Storage",
    "s3.bucket.name": "datalake",
    "s3.endpoint": "http://minio:9001",
    "s3.region": "us-east-1",
    "s3.ssl.enabled": "false",
    "s3.path.style.access": "true",
    "s3.access.key": "minioadmin",
    "s3.secret.key": "minioadmin",
    "aws.access.key.id": "minioadmin",
    "aws.secret.access.key": "minioadmin",
    "store.url": "http://minio:9001",
    "s3.credentials.provider.class": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner"
  }
}'





