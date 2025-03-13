import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from cassandra.util import uuid_from_time
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import lit, col
from cassandra.policies import RoundRobinPolicy
import uuid


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_jobs (
                    create_time timeuuid PRIMARY KEY,
                    ts timestamp, 
                    job_id int, 
                    custom_track text, 
                    bid int, 
                    campaign_id int, 
                    group_id int, 
                    publisher_id int);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    create_time = uuid_from_time(datetime.datetime.now())  # Tạo timeuuid
    bid = kwargs.get('bid', 0)
    ts = datetime.datetime.now()  # Timestamp hiện tại
    job_id = kwargs.get('job_id', 0)
    custom_track = kwargs.get('custom_track', '')
    campaign_id = kwargs.get('campaign_id', 0)
    group_id = kwargs.get('group_id', 0)
    publisher_id = kwargs.get('publisher_id', 0)

    try:
        session.execute("""
            INSERT INTO spark_streams.created_jobs (create_time, bid, ts, job_id, custom_track, 
                campaign_id, group_id, publisher_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (create_time, bid, ts, job_id, custom_track, campaign_id, group_id, publisher_id))

        logging.info(f"Data inserted: job_id={job_id}, campaign_id={campaign_id}")

    except Exception as e:
        logging.error(f'Could not insert data due to: {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'jobs_created') \
            .option('startingOffsets', 'earliest') \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        cluster = Cluster(
            ['localhost'],
            load_balancing_policy=RoundRobinPolicy()
        )

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    
    schema = StructType([
        StructField("create_time", StringType(), False),  
        StructField("ts", TimestampType(), False),
        StructField("job_id", IntegerType(), False),
        StructField("custom_track", StringType(), False),
        StructField("bid", IntegerType(), False),
        StructField("campaign_id", IntegerType(), False),
        StructField("group_id", IntegerType(), False),
        StructField("publisher_id", IntegerType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel

def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}...")
    batch_df.show()  


if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        spark_df = connect_to_kafka(spark_conn)

        if spark_df is None:
            logging.error("Kafka DataFrame is None! Kiểm tra kết nối Kafka.")
            exit(1)

        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)

            print("Streaming is being started...")


            streaming_query = (selection_df.writeStream
                               .foreachBatch(process_batch)
                               .format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_jobs')
                               .outputMode('append')
                               .start())
            
            streaming_query.awaitTermination()


