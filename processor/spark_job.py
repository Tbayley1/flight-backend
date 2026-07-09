import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, min, max, count, stddev
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.ml import PipelineModel, Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import psycopg2

# 1. THE FOOLPROOF SCHEMA
schema = StructType([
    StructField("from_airport_code", StringType(), True),
    StructField("from_country", StringType(), True),
    StructField("dest_airport_code", StringType(), True),
    StructField("dest_country", StringType(), True),
    StructField("aircraft_type", StringType(), True),
    StructField("airline_number", StringType(), True),
    StructField("airline_name", StringType(), True),
    StructField("flight_number", StringType(), True),
    StructField("departure_time", StringType(), True),
    StructField("arrival_time", StringType(), True),
    StructField("stops", StringType(), True),
    StructField("price", StringType(), True),
    StructField("co2_emissions", StringType(), True),
    StructField("avg_co2_emission_for_this_route", StringType(), True),
    StructField("scan_date", StringType(), True)
])

spark = SparkSession.builder.appName("FlightPriceProcessor").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

MODEL_PATH = "/app/ml_model"

# --- THE SELF-HEALING AUTOMATIC TRAINER ---
if not os.path.exists(MODEL_PATH):
    print("⚠️ ML Model not found! Initiating automatic emergency training...", flush=True)

    # 1. Create offline training data
    train_df = spark.createDataFrame([
        (0.0, 150.0, 500.0),
        (1.0, 200.0, 350.0),
        (2.0, 280.0, 200.0),
        (0.0, 130.0, 550.0),
        (1.0, 180.0, 320.0),
        (2.0, 250.0, 220.0),
    ], ["stops", "co2_emissions", "price"])

    # 2. Train the AI pipeline
    assembler = VectorAssembler(inputCols=["stops", "co2_emissions"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="price", maxIter=10)
    pipeline = Pipeline(stages=[assembler, lr])

    model = pipeline.fit(train_df)

    # 3. Save it to disk so it exists forever
    model.write().overwrite().save(MODEL_PATH)
    print("✅ Emergency training complete! Model saved locally.", flush=True)
# ------------------------------------------

print("🧠 Loading ML Model into Memory...", flush=True)
ml_model = PipelineModel.load(MODEL_PATH)

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "flight_data") \
    .option("startingOffsets", "latest") \
    .load()

# 2. FORCE CAST TO DOUBLE
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("price", col("price").cast("double")) \
    .withColumn("stops", col("stops").cast("double")) \
    .withColumn("co2_emissions", col("co2_emissions").cast("double"))

# Fill missing data
clean_df = json_df.na.fill({"stops": 0.0, "co2_emissions": 150.0})

# Predict!
ml_df = ml_model.transform(clean_df)
ml_df = ml_df.withColumnRenamed("prediction", "predicted_price")

# Aggregation (This is where the raw columns get merged into averages and minimums!)
agg_df = ml_df.groupBy("from_country", "dest_country") \
    .agg(
    avg("price").alias("avg_price"),
    min("price").alias("min_price"),
    max("price").alias("max_price"),
    stddev("price").alias("stddev_price"),
    avg("predicted_price").alias("predicted_price"),
    count("*").alias("flight_count")
)


def upsert_to_postgres(df, epoch_id):
    pdf = df.toPandas()
    if pdf.empty:
        return

    # --- THE HEADLIGHTS (DEBUG PRINT) FIXED ---
    print(f"\n--- BATCH {epoch_id}: SPARK VISION ---", flush=True)
    # This now simply prints exactly what Pandas sees, preventing the crash!
    print(pdf.head(), flush=True)
    print("------------------------------------\n", flush=True)

    conn = psycopg2.connect(dbname="flight_data", user="user", password="password", host="postgres", port="5432")
    cur = conn.cursor()

    upsert_query = """
        INSERT INTO route_value_stats (from_country, dest_country, avg_price, min_price, max_price, flight_count, predicted_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (from_country, dest_country)
        DO UPDATE SET 
            avg_price = EXCLUDED.avg_price, min_price = EXCLUDED.min_price,
            max_price = EXCLUDED.max_price, flight_count = EXCLUDED.flight_count,
            predicted_price = EXCLUDED.predicted_price, last_updated = CURRENT_TIMESTAMP;
    """
    main_data = pdf[['from_country', 'dest_country', 'avg_price', 'min_price', 'max_price', 'flight_count',
                     'predicted_price']].to_numpy()
    cur.executemany(upsert_query, [tuple(x) for x in main_data])

    valid_stats = pdf[pdf['stddev_price'] > 0].copy()
    if not valid_stats.empty:
        valid_stats['z_score'] = (valid_stats['min_price'] - valid_stats['avg_price']) / valid_stats['stddev_price']
        anomalies = valid_stats[valid_stats['z_score'] <= -2.0]

        if not anomalies.empty:
            print(f"🚨🚨 DETECTED {len(anomalies)} MARKET ANOMALIES! ROUTING TO ALERT TABLE 🚨🚨", flush=True)
            anomaly_query = """
                INSERT INTO market_anomalies (from_country, dest_country, min_price, avg_price, z_score, predicted_price)
                VALUES (%s, %s, %s, %s, %s, %s);
            """
            anomaly_data = anomalies[
                ['from_country', 'dest_country', 'min_price', 'avg_price', 'z_score', 'predicted_price']].to_numpy()
            cur.executemany(anomaly_query, [tuple(x) for x in anomaly_data])

    conn.commit()
    cur.close()
    conn.close()


# Checkpoint v9 to ensure a clean start with the new print statement
query = agg_df.writeStream \
    .foreachBatch(upsert_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/spark_checkpoints_v9") \
    .start()

query.awaitTermination()