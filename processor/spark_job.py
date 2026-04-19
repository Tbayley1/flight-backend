from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, min, max, count
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# 1. Schema
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
    .option("subscribe", "flight_data") \
    .option("startingOffsets", "earliest") \
    .load()

# 3. Transform (No trim used here)
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 4. Route Value Aggregation
# Grouping by route to find live pricing trends
agg_df = json_df.groupBy("from_country", "dest_country") \
    .agg(
    avg("price").alias("avg_price"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    count("*").alias("flight_count")
)

# Sort by the average price (Cheapest routes first)
final_sorted_df = agg_df.orderBy(col("avg_price").asc())


def write_to_postgres(df, epoch_id):
    print(f"--- UPDATING ROUTE MARKET DATA: BATCH {epoch_id} ---", flush=True)

    df.show()
    print(f"ROW COUNT: {df.count()}", flush=True)

    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/flight_data") \
        .option("dbtable", "route_value_stats") \
        .option("user", "user") \
        .option("password", "password") \
        .option("driver", "org.postgresql.Driver") \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    print(f"--- ROUTE DATA REFRESHED ---", flush=True)


# 5. Output (Complete Mode)
query = final_sorted_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/spark_checkpoints") \
    .start()

query.awaitTermination()