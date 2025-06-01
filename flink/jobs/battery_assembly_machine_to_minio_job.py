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
            df['welding_current_amps'] = df['readings'].apply(lambda x: x.get('welding_current_amps'))
            df['welding_voltage_volts'] = df['readings'].apply(lambda x: x.get('welding_voltage_volts'))
            df['electrode_temperature_celsius'] = df['readings'].apply(lambda x: x.get('electrode_temperature_celsius'))
            df['cooling_system_temperature_celsius'] = df['readings'].apply(lambda x: x.get('cooling_system_temperature_celsius'))
            df['alignment_accuracy_mm'] = df['readings'].apply(lambda x: x.get('alignment_accuracy_mm'))
            df['vacuum_pressure_kpa'] = df['readings'].apply(lambda x: x.get('vacuum_pressure_kpa'))
            df['cell_voltage_uniformity_ratio'] = df['readings'].apply(lambda x: x.get('cell_voltage_uniformity_ratio'))
            df['insulation_resistance_mohm'] = df['readings'].apply(lambda x: x.get('insulation_resistance_mohm'))
            df['leakage_current_ma'] = df['readings'].apply(lambda x: x.get('leakage_current_ma'))
            df['operation_time_hours'] = df['readings'].apply(lambda x: x.get('operation_time_hours'))
            df['batteries_produced'] = df['readings'].apply(lambda x: x.get('batteries_produced'))
            df['electrode_wear_percent'] = df['readings'].apply(lambda x: x.get('electrode_wear_percent'))
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
        topics="battery_assembly_machine_sensor",
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
            bucket="datalake/sensor_data/battery_assembly_machine_sensor",
            batch_size=10
        )
    ).name("Buffered Write to MinIO")

    # Execute the pipeline
    env.execute("battery assembly machine to Minio job")

if __name__ == "__main__":
    main()