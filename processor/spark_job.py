from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, to_timestamp, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# 1. New Schema based on your headers
schema = StructType([
    StructField("from_airport_code", StringType(), True),
    StructField("from_country", StringType(), True),
    StructField("dest_airport_code", StringType(), True),
    StructField("dest_country", StringType(), True),
    StructField("aircraft_type", StringType(), True),
    StructField("airline_number", StringType(), True),
    StructField("airline_name", StringType(), True),
    StructField("flight_number", FloatType(), True),
    StructField("departure_time", StringType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("stops", FloatType(), True),
    StructField("price", FloatType(), True),
    StructField("co2_emissions", FloatType(), True),
    StructField("avg_co2_emission_for_this_route", FloatType(), True),
    StructField("scan_date", StringType(), True)

])

spark = SparkSession.builder.appName("FlightPriceProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Read from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "flight_data")\
    .load()


# 3. Transform
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
cleaned_df = json_df.withColumn(
    # Convert String to proper Timestamp using your specific format
    "departure_time", to_timestamp(col("departure_time"), "dd-MM-yyyy HH:mm")
)


# 4. Aggregate: Average Price per Airline
# (We use airline_name because it's in your CSV)
agg_df = json_df.groupBy("airline_name").agg(
    avg("price").alias("avg_price"),
   )


def write_to_postgres(df, epoch_id):
    print(f"--- ATTEMPTING TO WRITE BATCH {epoch_id} TO POSTGRES ---", flush=True)

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/flight_data") \
        .option("dbtable", "airline_stats") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print(f"--- SUCCESSFULLY WROTE BATCH {epoch_id} ---", flush=True)


checkpoint_path = "/tmp/spark_checkpoints"
# 5. Output
query = agg_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()