import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MaxTemperatures").getOrCreate()

schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("./datasets/netflix/1800.csv")
df.printSchema()

# Filter out all but TMAX entries
maxTemps = df.filter(df.measure_type == "TMAX")

# Select only stationID and temperature
stationTemps = maxTemps.select("stationID", "temperature")

# Aggregate to find maximum temperature for every station
maxTempsByStation = stationTemps.groupBy("stationID").max("temperature")
maxTempsByStation.show()

# Convert temperature to fahrenheit and sort the dataset
maxTempsByStationF = maxTempsByStation.withColumn("temperature",
                                                  func.round(func.col("max(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                  .select("stationID", "temperature").sort("temperature")
                                                  
# Collect, format, and print the results
results = maxTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
    
spark.stop()

                                                  
