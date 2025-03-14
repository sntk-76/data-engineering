from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.common.time import Duration
from pyflink.datastream.checkpointing import CheckpointConfig


def create_events_source_kafka(t_env):
    """
    Create a Flink table linked to the Kafka topic 'green-trips'.
    """
    table_name = "processed_events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            WATERMARK FOR lpep_dropoff_datetime AS lpep_dropoff_datetime - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_events_aggregated_sink(t_env):
    """
    Create a Flink table linked to the PostgreSQL database.
    """
    table_name = 'processed_events_aggregated'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INT,
            DOLocationID INT,
            streak_length BIGINT,
            PRIMARY KEY (PULocationID, DOLocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    """
    Flink job to read from Kafka, process data, and write results to PostgreSQL.
    """
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)  # Adjust parallelism

    # ✅ Use FileSystem Checkpoint Storage with Correct Import
    env.enable_checkpointing(10 * 1000)  # Checkpoint every 10 seconds
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpoint_storage("file:///tmp/flink-checkpoints")

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source table
        source_table = create_events_source_kafka(t_env)

        # Create PostgreSQL sink table
        aggregated_table = create_events_aggregated_sink(t_env)

        # Query to calculate streak_length using session windows
        query = f"""
        INSERT INTO {aggregated_table}
        SELECT
            PULocationID,
            DOLocationID,
            COUNT(*) AS streak_length
        FROM {source_table}
        GROUP BY PULocationID, DOLocationID,
                 SESSION(lpep_dropoff_datetime, INTERVAL '5' MINUTE);
        """
        t_env.execute_sql(query).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()
