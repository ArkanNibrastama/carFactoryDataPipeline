import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
import s3fs
from datetime import datetime
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction

def transform_debezium_message(msg):
    try:
        parsed = json.loads(msg)
        before = parsed.get("before")
        after = parsed.get("after")

        if before is None and after is not None:
            return {
                "id": after["id"],
                "created_at": datetime.utcfromtimestamp(after["created_at"] / 1_000_000).strftime('%Y-%m-%d %H:%M:%S'),
                "name": after["name"],
                "stock": after["stock"],
                "is_created": 1,
                "is_updated": 0,
                "is_deleted": 0,
            }
        elif before is not None and after is not None:
            return {
                "id": after["id"],
                "created_at": datetime.utcfromtimestamp(after["created_at"] / 1_000_000).strftime('%Y-%m-%d %H:%M:%S'),
                "name": after["name"],
                "stock": after["stock"],
                "is_created": 0,
                "is_updated": 1,
                "is_deleted": 0,
            }
        elif before is not None and after is None:
            return {
                "id": before["id"],
                "created_at": datetime.utcfromtimestamp(before["created_at"] / 1_000_000).strftime('%Y-%m-%d %H:%M:%S'),
                "name": before["name"],
                "stock": before["stock"],
                "is_created": 0,
                "is_updated": 0,
                "is_deleted": 1,
            }
        else:
            return None
    except Exception as e:
        print(f"Error parsing message: {e}")
        return None

class BufferedWriteMinIOSink(MapFunction):
    def __init__(self, endpoint, access_key, secret_key, bucket, batch_size=100):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.batch_size = batch_size
        self.buffer = []
        self.client = None  # tidak perlu lock
    
    def open(self, runtime_context):
        self.fs = s3fs.S3FileSystem(
            key=self.access_key,
            secret=self.secret_key,
            client_kwargs={'endpoint_url': self.endpoint}
        )
    
    def map(self, value):
        self.buffer.append(json.loads(value))
        
        if len(self.buffer) >= self.batch_size:
            self._flush_buffer()
        return value  # return diperlukan karena ini MapFunction, meskipun datanya tidak dipakai
    
    def _flush_buffer(self):
        if self.buffer:
            df = pd.DataFrame(self.buffer)
            df["date"] = pd.to_datetime(df["created_at"]).dt.strftime('%Y-%m-%d')
            partition_date = df["date"].iloc[0]
            table = pa.Table.from_pandas(df.drop(columns=["date"]))
            
            filename = f"logistics_{uuid.uuid4().hex[:8]}.parquet"
            path = f"{self.bucket}/date={partition_date}/{filename}"  # jangan pakai s3a:// disini karena s3fs udah handle
            
            with self.fs.open(path, 'wb') as f:
                pq.write_table(table, f)
            
            print(f"Flushed batch of {len(self.buffer)} records to {path}")
            self.buffer.clear()
    
    def close(self):
        if self.buffer:
            self._flush_buffer()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(60000)  # checkpoint every 60 seconds

    # Add Kafka connector JAR
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.2.0-1.18.jar")

    # Setup Kafka consumer
    consumer = FlinkKafkaConsumer(
        topics='source.public.logistics',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'logistics-consumer',
            'auto.offset.reset': 'earliest'  # Start from beginning if no offset
        }
    )

    # Create data stream
    stream = env.add_source(consumer, "Kafka Source")

    # Transform Debezium messages
    transformed = stream \
        .map(transform_debezium_message, output_type=Types.PICKLED_BYTE_ARRAY()) \
        .name("Transform Debezium") \
        .filter(lambda d: d is not None) \
        .name("Filter Non-null") \
        .map(lambda d: json.dumps(d), output_type=Types.STRING()) \
        .name("Serialize to JSON")
        
    # transformed.print("Transformed Data")
    
    transformed.map(
        BufferedWriteMinIOSink(
            endpoint="http://minio:9001",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="datalake/logistics",
            batch_size=100
        )
    ).name("Buffered Write to MinIO")

    # Execute the job
    env.execute("Kafka Logistics Debezium to MinIO Parquet")


if __name__ == "__main__":
    main()