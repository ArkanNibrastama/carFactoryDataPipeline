import logging
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions
from pyflink.common.typeinfo import Types

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Add required JAR files with correct versions
    env.add_jars("file:///opt/flink/lib/flink-connector-jdbc-3.2.0-1.19.jar")
    env.add_jars("file:///opt/flink/lib/clickhouse-jdbc-0.6.0.jar")

    # Define the type info using TUPLE type
    type_info = Types.ROW([
        Types.STRING(),  # prediction_timestamp
        Types.STRING(),  # machine_id
        Types.STRING(),  # machine_type
        Types.FLOAT(),   # failure_probability
        Types.STRING()   # risk_level
    ])
    
    # Create a test data stream
    ds = env.from_collection(
        [
            ("2025-04-01", "ARM-01", "ARM-ROBOT", 0.332, "LOW"),
            ("2025-04-01", "ARM-02", "ARM-ROBOT", 0.543, "MEDIUM"),
            ("2025-04-01", "ARM-03", "ARM-ROBOT", 0.788, "HIGH"),
        ],
        type_info=type_info
    )
    
    # wonder if I can create stream map here?

    # Create JDBC connection options with updated driver and URL
    jdbc_connection_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_url('jdbc:ch://localhost:8123/stream_data_warehouse') \
        .with_driver_name('com.clickhouse.jdbc.ClickHouseDriver') \
        .with_user_name('admin') \
        .with_password('admin') \
        .build()
    
    # Create and add JDBC sink
    ds.add_sink(
        JdbcSink.sink(
            sql="INSERT INTO sensor_data (prediction_timestamp, machine_id, machine_type, failure_probability, risk_level) VALUES (?, ?, ?, ?, ?)",
            type_info=type_info,
            jdbc_connection_options=jdbc_connection_options
        )
    )
    
    # Execute the job
    env.execute("test2")

if __name__ == "__main__":
    main()