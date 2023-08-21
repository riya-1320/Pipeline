from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, col, struct
from pyspark.sql.types import *
spark = SparkSession \
    .builder \
    .appName("Pipeline") \
    .config('spark.driver.bindAddress', '127.0.0.1') \
    .master('local[*]') \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

checkPointLocation = '/home/xs354-riygup/TASKS/finalTask/checkPoint'
csvPath = '/home/xs354-riygup/TASKS/finalTask'

preDefinedSchema = (
    StructType()
    .add("Date/Time", StringType())
    .add('LV ActivePower (kW)', DoubleType())
    .add('Wind Speed (m/s)', DoubleType())
    .add('Theoretical_Power_Curve (KWh)', DoubleType())
    .add('Wind Direction (°)', DoubleType())
)

# Read CSV file in streaming fashion
csvDf = spark \
    .readStream \
    .option('header', 'true') \
    .schema(preDefinedSchema) \
    .csv(csvPath)

# publish csvDf into kafka in streaming fashion
jsonDf = csvDf.select(to_json(struct(col("*"))).alias("value"))
publishDf = jsonDf.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "turbineData") \
    .outputMode("append") \
    .option("checkPointLocation", checkPointLocation) \
    .start()
publishDf.awaitTermination()


