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
# groupVideos = selectVideos.groupBy("title").agg(func.max("views").alias('topviews'))
groupVideos = selectVideos.groupBy("title").agg(func.max("views").alias('views'))

# sort the videos in descending order base on views
# topVideos = groupVideos.orderBy(func.desc("topviews")).limit(10)
topVideos = groupVideos.orderBy(func.desc("views")).limit(10)
# topVideos.show()

# use right join to attach thumbnail_link in the table
# duplicated title column
# right_join = selectVideos.join(topVideos,(topVideos.title == selectVideos.title) & \
#                                 (topVideos.topviews == selectVideos.views), \
#                                 how='right')
# sortedTopVideo = right_join.orderBy(func.desc("views")).limit(10)
# sortedTopVideo.show()

# use inner join based on the title and views
# no duplicated columns
inner_join = selectVideos.join(topVideos, ['title','views'])
sortedTopVideo = inner_join.orderBy(func.desc("views")).limit(10)
sortedTopVideo.show(10,False)

# Stop the session
spark.stop()