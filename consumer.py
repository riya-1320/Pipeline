from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, substring, to_timestamp, concat, lit, from_json, month, hour, when
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("pipeline") \
    .config('spark.driver.bindAddress', '127.0.0.1') \
    .config("spark.jars", "/home/xs354-riygup/spark/spark-3.3.1-bin-hadoop3/jars/postgresql-42.2.6.jar") \
    .master('local[*]') \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

checkPointLocation = '/home/xs354-riygup/TASKS/finalTask/checkPoint/checkPointConsume'

preDefinedSchema = (
    StructType()
    .add("Date/Time", StringType())
    .add('LV ActivePower (kW)', DoubleType())
    .add('Wind Speed (m/s)', DoubleType())
    .add('Theoretical_Power_Curve (KWh)', DoubleType())
    .add('Wind Direction (°)', DoubleType())
)

# write csvDf into kafka in streaming fashion
writeDf = spark \
    .readStream \
    .format("kafka") \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'turbineData') \
    .load()

# transformed schema
convertString = writeDf \
    .selectExpr('CAST(value As STRING)')

dataFrame = convertString \
    .select(from_json(col('value'), preDefinedSchema).alias('data')) \
    .select('data.*')

# Data Cleansing: Handling Null Values
dataFrame = dataFrame.dropna()

# Data Validation

dataFrame = dataFrame.filter(col("LV ActivePower (kW)") >= 0)
dataFrame = dataFrame.filter(col("Wind Speed (m/s)") >= 0)

# transformations on dataframe:
dataFrame1 = dataFrame.withColumn("Date", to_date(col("Date/Time"), "dd MM yyyy HH:mm"))
dataFrame1 = dataFrame1.withColumn("time", substring(col("Date/Time"), 12, 5)).withColumn('hour', hour(col('time')))
dataFrame1 = dataFrame1.withColumn("signal_ts",
                                   to_timestamp(concat(col("Date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm")).drop(
    'time')
dataFrame1 = dataFrame1.withColumn("Month", month(col("Date")).cast(StringType())).drop('Date/Time')

# Add power generation indicator
dataFrame2 = (dataFrame1.withColumn("generation_indicator", when((col('LV ActivePower (kW)') < 200), lit("Low"))
                                    .otherwise(
    when((col('LV ActivePower (kW)') >= 200) & (col('LV ActivePower (kW)') <= 600), lit("Medium"))
    .otherwise(when((col('LV ActivePower (kW)') >= 600) & (col('LV ActivePower (kW)') <= 1000), lit("High"))
    .otherwise(
        when((col('LV ActivePower (kW)') >= 1000), lit("Exceptional")))))))

# Add wind speed indicator
dataFrame3 = (dataFrame1.withColumn("windSpeedIndicator", when((col('Wind Speed (m/s)') < 5), lit("Low"))
                                    .otherwise(
    when((col('Wind Speed (m/s)') >= 5) & (col('Wind Speed (m/s)') <= 15), lit("Medium"))
    .otherwise(when((col('Wind Speed (m/s)') >= 15) & (col('Wind Speed (m/s)') <= 25), lit("High"))
    .otherwise(
        when((col('Wind Speed (m/s)') >= 25), lit("Exceptional")))))))


def write_to_postgres(df, epoch_id):
    mode = "append"
    url = "jdbc:postgresql://10.0.0.27:5432/riyaTurbineData"
    properties = {"user": "superset", "password": "superset", "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=url, table='table4', mode=mode, properties=properties)


dataFrame3.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", checkPointLocation) \
    .start() \
    .awaitTermination()
















