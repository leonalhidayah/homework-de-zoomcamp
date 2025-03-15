from pyflink.common.time import Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.expressions import col
from pyflink.table.window import Session


def process_stream():
    # 1️⃣ Setup Flink Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)  # Checkpoint setiap 10 detik
    env.set_parallelism(3)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # 2️⃣ Buat sumber data dari Kafka
    t_env.execute_sql("""
        CREATE TABLE trips (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            lpep_dropoff_datetime STRING,
            event_time AS TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # 3️⃣ Buat sink di PostgreSQL
    t_env.execute_sql("""
        CREATE TABLE longest_streak (
            PULocationID INTEGER,
            DOLocationID INTEGER,
            trip_count BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'longest_streak',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        )
    """)

    # 4️⃣ Gunakan PyFlink Table API untuk Session Window
    table = t_env.from_path("trips")

    session_window = table.window(
        Session.with_gap(Duration.of_minutes(5)).on(col("event_time")).alias("w")
    )

    result = (
        table.window(session_window)
        .group_by(col("w"), col("PULocationID"), col("DOLocationID"))
        .select(
            col("PULocationID"),
            col("DOLocationID"),
            col("w").end.alias("window_end"),
            col("*").count.alias("trip_count"),
        )
    )

    # 5️⃣ Tulis hasil ke PostgreSQL
    result.execute_insert("longest_streak").wait()


if __name__ == "__main__":
    process_stream()
