from pyflink.table import StreamTableEnvironment

from job.green_trips_common import create_env, create_green_trips_source_kafka


def create_sink_table(t_env: StreamTableEnvironment) -> str:
    table_name = "green_trips_tip_hourly"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            total_tip_amount DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
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


def run() -> None:
    t_env = create_env(parallelism=1)

    source_table = create_green_trips_source_kafka(t_env)
    sink_table = create_sink_table(t_env)

    t_env.execute_sql(
        f"""
        INSERT INTO {sink_table}
        SELECT
            window_start,
            SUM(tip_amount) AS total_tip_amount
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_timestamp), INTERVAL '1' HOUR)
        )
        GROUP BY window_start
        """
    ).wait()


if __name__ == "__main__":
    run()
