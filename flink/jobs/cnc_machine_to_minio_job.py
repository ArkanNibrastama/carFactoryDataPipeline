import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from datetime import datetime
import pandas as pd
import s3fs
from pyflink.datastream.functions import MapFunction
import json
import pyarrow as pa
import pyarrow.parquet as pq
from pyflink.common.serialization import SimpleStringSchema


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
class BufferedWriteMinIOSink(MapFunction):
    def __init__(self, endpoint, access_key, secret_key, bucket, batch_size=100):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket
        self.batch_size = batch_size
        self.buffer = []
        self.client = None 
    
    def open(self, runtime_context):
        self.fs = s3fs.S3FileSystem(
            key=self.access_key,
            secret=self.secret_key,
            client_kwargs={'endpoint_url': self.endpoint}
        )
    
    def map(self, value):
        self.buffer.append(json.loads(value))
        print(f"append: {json.loads(value)}")
        
        if len(self.buffer) >= self.batch_size:
            self._flush_buffer()
        return value  # return diperlukan karena ini MapFunction, meskipun datanya tidak dipakai
    
    def _flush_buffer(self):
        if self.buffer:
            df = pd.DataFrame(self.buffer)
            
            # transform here
            df['spindle_speed_rpm'] = df['readings'].apply(lambda x: x.get('spindle_speed_rpm'))
            df['cutting_force_n'] = df['readings'].apply(lambda x: x.get('cutting_force_n'))
            df['motor_temperature_celsius'] = df['readings'].apply(lambda x: x.get('motor_temperature_celsius'))
            df['coolant_temperature_celsius'] = df['readings'].apply(lambda x: x.get('coolant_temperature_celsius'))
            df['coolant_flow_rate_l_min'] = df['readings'].apply(lambda x: x.get('coolant_flow_rate_l_min'))
            df['vibration_mm_per_s'] = df['readings'].apply(lambda x: x.get('vibration_mm_per_s'))
            df['tool_wear_percent'] = df['readings'].apply(lambda x: x.get('tool_wear_percent'))
            df['operation_time_hours'] = df['readings'].apply(lambda x: x.get('operation_time_hours'))
            df['parts_produced'] = df['readings'].apply(lambda x: x.get('parts_produced'))
            df.drop(columns=['readings'], inplace=True)

            table = pa.Table.from_pandas(df)
            
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            datenow = datetime.now().strftime("%Y%m%d")
            path = f"{self.bucket}/{datenow}/{timestamp}.parquet"  # jangan pakai s3a:// disini karena s3fs udah handle
            
            with self.fs.open(path, 'wb') as f:
                pq.write_table(table, f)
            
            print(f"Flushed batch of {len(self.buffer)} records to {path}")
            self.buffer.clear()
    
    def close(self):
        if self.buffer:
            self._flush_buffer()


def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Add Kafka connector dependencies
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-3.2.0-1.18.jar")

    kafka_consumer = FlinkKafkaConsumer(
        topics="cnc_machine_sensor",
        deserialization_schema=SimpleStringSchema(),
        properties={
            "bootstrap.servers": "kafka:9092",
            "group.id": "flink-consumer-group-string",
        },
    )

    stream_to_minio = env.add_source(kafka_consumer)

    # load to Minio
    stream_to_minio.map(
        BufferedWriteMinIOSink(
            endpoint="http://minio:9001",
            access_key="minioadmin",
            secret_key="minioadmin",
            bucket="datalake/sensor_data/cnc_machine_sensor",
            batch_size=10
        )
    ).name("Buffered Write to MinIO")

    # Execute the pipeline
    env.execute("cnc machine to Minio job")

if __name__ == "__main__":
    main()