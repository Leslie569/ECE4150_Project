import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import SQLContext

sc = SparkContext("local", "Stock Analysis - ECE4150")
sqlContext = SQLContext(sc)

txt = sc.textFile("./datasets/Stocks/aapl.us.txt")
header = txt.first()
schemaString = header.replace('"','')
newschema = [StructField(field_name, StringType(), True) for field_name in schemaString.split(',')][:-1]

for i in range(1, len(newschema)):
    newschema[i].dataType = FloatType()

schema = StructType(newschema)

header_ref = txt.filter(lambda l: "Open" in l)
header_ref.collect()
txt = txt.subtract(header_ref)
txt_processed = txt.map(lambda l: l.split(",")).map(lambda d: (d[0], float(d[1]), float(d[2]), float(d[3]), float(d[4]), float(d[5])))

df = sqlContext.createDataFrame(txt_processed, schema)


all_days = df.count()
up_days = df.filter(df.Open < df.Close).count()
flat_days = df.filter(df.Open == df.Close).count()
down_days = df.filter(df.Open > df.Close).count()



print(all_days)
print(up_days)
print(flat_days)
print(down_days)