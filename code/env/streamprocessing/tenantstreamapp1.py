from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, unix_timestamp, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("tenantstreamapp1") \
    .master("local[*]") \
    .getOrCreate()

# set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

# define the schema for the incoming data
schema = StructType([
    StructField("time", StringType()),
    StructField("readable_time", TimestampType()),
    StructField("acceleration", DoubleType()),
    StructField("acceleration_x", DoubleType()),
    StructField("acceleration_y", DoubleType()),
    StructField("acceleration_z", DoubleType()),
    StructField("battery", IntegerType()),
    StructField("humidity", IntegerType()),
    StructField("pressure", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("dev_id", StringType())
])

# read binary data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "tenant1-data") \
    .load()

# convert the value column to string and decode it according to schema
#df = df \
#    .withColumn("value", col("value").cast("string")) \
#    .withColumn("value", from_json("value", schema)) \
#    .select(col("value.*"))

df = df \
.select(
    from_json(
        decode(col("value"), "utf-8"),
        schema
    ).alias("value")
)



# Check for null values in time column (wrong data), if data is incorrect, then warn tenant by sending to warn topic
df_null = df.filter(col("time").isNull()) #| col("readable_time").isNull() | col("acceleration").isNull | col("dev_id").isNull() | col("battery").isNull() | col("pressure").isNull() | col("temperature").isNull() | col("humidity").isNull() | col("acceleration_x").isNull() | col("acceleration_y").isNull() | col("acceleration_z").isNull())
query_warn_null = df_null \
    .selectExpr("CAST(dev_id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "tenant1-warn") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint-null") \
    .start()





# define the windowing, calculate the averages of the data
windowedAvg = df \
    .withWatermark("readable_time", "1 minutes") \
    .groupBy(window("readable_time", "1 minutes"), "dev_id") \
    .agg(avg("temperature"), avg("humidity"), avg("acceleration"), avg("pressure"), avg("battery"), max("readable_time").cast("long").alias("max_readable_time"), count("*").alias("count")) \
    .select("window.start", "window.end", "avg(acceleration)", "avg(temperature)", "avg(humidity)", "avg(pressure)", "avg(battery)", "max_readable_time", "count", col("dev_id").alias("dev_id"))

# calculate the latency and throughput
outputData = windowedAvg \
    .withColumn("latency", (col("max_readable_time") - unix_timestamp(col("start"))) / 1000) \
    .withColumn("throughput", col("count") / 60)


# Test if environment variables are ok, if not, send to warn topic
df_environment_warn = windowedAvg.filter(
    (col("avg(temperature)") > 30) | (col("avg(temperature)") < 20) |
    (col("avg(humidity)") < 90) |
    (col("avg(battery)") < 5) |
    (col("avg(pressure)") > 1000) | (col("avg(pressure)") < 900)) \
        .select(to_json(struct(col("dev_id"), col("start"), col("end"), col("avg(acceleration)"), col("avg(temperature)"), col("avg(humidity)"), col("avg(pressure)"),  col("avg(battery)"))).alias("value"))

query_warn = df_environment_warn \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "tenant1-warn") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .start()





# write the output to the data-output topic to the tenant 
query = outputData \
    .select(to_json(struct(col("dev_id"), col("start"), col("end"), col("avg(acceleration)"), col("avg(temperature)"), col("avg(humidity)"), col("avg(pressure)"),  col("avg(battery)"), col("latency"), col("throughput"))).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "tenant1-data-output") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint") \
    .start()


# wait for the streams to finish
query.awaitTermination()
query_warn.awaitTermination()
query_warn_null.awaitTermination()
