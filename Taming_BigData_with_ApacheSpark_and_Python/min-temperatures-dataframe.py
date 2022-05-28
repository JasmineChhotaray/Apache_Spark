from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([ #\
    StructField("stationID", StringType(), True), #\
    StructField("date", IntegerType(), True), #\
    StructField("measure_type", StringType(), True), #\
    StructField("temperature", FloatType(), True)])

# Read the file as a dataframe
df = spark.read.schema(schema).csv("E:/Projects/SparkCourse/data/1800.csv")
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
minTempByStation = stationTemps.groupBy("stationID").min("temperature")
minTempByStation.show()

# Convert temperature to fahrenheit and sort the dataset
minTempByStationF = minTempByStation.withColumn("temperature",
                            func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                            .select("stationID", "temperature").sort("temperature")

# Collect, format and print the results
results = minTempByStationF.collect()

for result in results:
    # print(result[0] + "\t{:.2f}F".format(result[1]))
    print(f"{result[0]} \t {result[1]:.2f}F")

spark.stop()
