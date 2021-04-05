import findspark
findspark.init()

from pyspark import SparkContext

sc = SparkContext("local", "stock analysis")

lines = sc.textFile("/datasets/Stocks/AAPL.txt")

llist = lines.collect()

for line in llist:
    print(line)