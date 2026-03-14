from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_processed_events_sink_postgres(t_env):
    # Define a JavaDataBaseConnectivity sink table in PostgreSQL where processed rides will be stored.
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
            total_amount DOUBLE,
            pickup_datetime TIMESTAMP
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    # Register the sink table in the Table Environment.
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    # Define a Kafka source table that reads JSON messages from the "rides" topic.
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_distance DOUBLE,
            total_amount DOUBLE,
            tpep_pickup_datetime BIGINT
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'rides',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    # Register the source table in the Table Environment.
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    # Set up the execution environment (DataStream API).
    env = StreamExecutionEnvironment.get_execution_environment()
    # Enable periodic checkpoints for fault tolerance (every 10 seconds).
    env.enable_checkpointing(10 * 1000)

    # Set up the Table API environment in streaming mode.
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    try:
        # Create Kafka source and PostgreSQL sink tables.
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)
        # Insert rows from Kafka into PostgreSQL, converting epoch millis to TIMESTAMP.
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        PULocationID,
                        DOLocationID,
                        trip_distance,
                        total_amount,
                        TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3) as pickup_datetime
                    FROM {source_table}
                    """
        ).wait()

    except Exception as e:
        # Print any error that occurs during the streaming insert.
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    # Run the streaming job.
    log_processing()
