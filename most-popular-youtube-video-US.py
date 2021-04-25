import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("PopularYoutubeVideos").getOrCreate()

# Create schema when reading USvideos.csv
schema = StructType([ \
                     StructField("video_id", StringType(), True), \
                     StructField("trending_date", StringType(), True), \
                     StructField("title", StringType(), True), \
                     StructField("views", IntegerType(), True), \
                     StructField("likes", IntegerType(), True), \
                     StructField("dislikes", IntegerType(), True), \
                     StructField("comment_count", IntegerType(), True), \
                     StructField("thumbnail_link", StringType(), True), \
                     StructField("description", StringType(), True)
                    ])

# Load up movie data as dataframe
videosDF = spark.read.option("sep", ",").schema(schema).csv("./datasets/USvideos.csv")
# videosDF.printSchema()

# Select only video title and views
selectVideos = videosDF.select("title", "views", "thumbnail_link")

# group the videos base on the highest views
groupVideos = selectVideos.groupBy("title").max("views")

# sort the videos in descending order base on views
topVideos = groupVideos.orderBy(func.desc("max(views)"))

# Show the top 10
topVideos.show(10)

# Stop the session
spark.stop()