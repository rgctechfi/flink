from pyflink.table import StreamTableEnvironment

from job.green_trips_common import create_env, create_green_trips_source_kafka

def create_sink_table(t_env: StreamTableEnvironment) -> str:
    table_name = "green_trips_session_5m"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            PULocationID INT,
            num_trips BIGINT,
            PRIMARY KEY (window_start, window_end, PULocationID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """
    t_env.execute_sql(sink_ddl)
    return table_name

def run() -> None:
    t_env = create_env(parallelism=1)

    source_table = create_green_trips_source_kafka(t_env)
    sink_table = create_sink_table(t_env)

    t_env.execute_sql(
        f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            window_end,
            PULocationID,
            COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(
                -- Important: use PARTITION BY inside the TABLE clause (not as a separate argument),
                -- otherwise Flink SQL parsing fails.
                TABLE {source_table} PARTITION BY PULocationID,
                DESCRIPTOR(event_timestamp),
                INTERVAL '5' MINUTE
            )
        )
        GROUP BY window_start, window_end, PULocationID
        """
    ).wait()

if __name__ == "__main__":
    run()
