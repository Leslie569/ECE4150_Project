import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


import boto3
from env import *
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb', aws_access_key_id=AWS_ACCESS_KEY,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                            region_name=AWS_REGION)

table_output = dynamodb.Table("superhero")



spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("./datasets/netflix/Marvel+Names.txt")

lines = spark.read.text("./datasets/netflix/Marvel+Graph.txt")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
mostPopular = connections.sort(func.col("connections").desc()).head(10)

stats = []
for hero in mostPopular:
    name = names.filter(func.col("id") == hero[0]).select("name").first()[0]
    num = hero[1]
    stats.append((name, num))
    table_output.put_item(
        Item={
            "name": name,
            "number_of_appearance": num
        })
print(stats)