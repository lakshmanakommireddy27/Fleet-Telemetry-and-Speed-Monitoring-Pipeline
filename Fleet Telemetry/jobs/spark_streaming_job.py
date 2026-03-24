from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
import os
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()

    # Kafka configurations
    TOPIC = os.getenv('TOPIC')
    BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS')

    # Postgres configurations
    # HOST = os.getenv('HOST')
    # PORT =  os.getenv('PORT')
    TABLE = os.getenv('TABLE')
    URL = os.getenv('URL')
    DRIVER = os.getenv('DRIVER')
    USER = os.getenv('USER')
    PASSWORD = os.getenv('PASSWORD')

    # Create Spark Session
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .getOrCreate()

    # Configure Kafka Stream
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .load()

    # Define schema for JSON data
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("location", StringType(), True),
        StructField("vehicle_info", StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("color", StringType(), True),
            StructField("year", StringType(), True)
        ])),
        StructField("speed", DoubleType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("speed_limit", DoubleType(), True),
        StructField("traffic_density", StringType(), True),
        StructField("road_condition", StringType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("incident_reported", BooleanType(), True),
        StructField("incident_details", StringType(), True)
    ])

    # Parse JSON data
    vehicle_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Extract vehicle information
    vehicle_df = vehicle_df.select(
        "timestamp", "location", "speed", "speed_limit", "vehicle_type",
        "traffic_density", "road_condition", "weather_condition",
        "incident_reported", "incident_details",
        col("vehicle_info.vehicle_id").alias("vehicle_id"),
        col("vehicle_info.make").alias("make"),
        col("vehicle_info.model").alias("model"),
        col("vehicle_info.color").alias("color"),
        col("vehicle_info.year").alias("year")
    )

    # Create a temporary view for Spark SQL
    vehicle_df.createOrReplaceTempView("vehicle_data")

    # Find vehicles that exceed speed limit
    violations_df = spark.sql("""
        SELECT *
        FROM vehicle_data
        WHERE speed > speed_limit
    """)

    # Function to write data to PostgreSQL
    def write_to_postgresql(df, epoch_id):
        df.write \
            .format('jdbc') \
            .options(
                url=URL,
                driver=DRIVER,
                dbtable=TABLE,
                user=USER,
                password=PASSWORD
            ) \
            .mode('append') \
            .save()

    # Write raw data to HDFS
    query_vehicle_df = vehicle_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/vehicle-data") \
        .option("path", "hdfs://namenode:8020/raw_data/vehicle-data") \
        .start()

    # Write violations data to PostgreSQL
    query_violations_df = violations_df.writeStream \
        .foreachBatch(write_to_postgresql) \
        .outputMode('update') \
        .option("checkpointLocation", "checkpoint") \
        .option("failOnDataLoss", "false") \
        .start()

    # Await termination of queries
    query_vehicle_df.awaitTermination()
    query_violations_df.awaitTermination()

# Entry point
if __name__ == "__main__":
    main()
