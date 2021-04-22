import pyspark
from pyspark.ml.regression import LinearRegression
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler



#SparkSession is now the entry point of Spark
#SparkSession can also be construed as gateway to spark libraries
sc = SparkContext('local')
spark = SparkSession(sc)

#create instance of spark class
spark=SparkSession.builder.appName('covid_vaccine_model').getOrCreate()

#create spark dataframe of input csv file
df=spark.read.csv('country_vaccinations.csv'
                  ,inferSchema=True,header=True)

#in this analytic program, we are looking for vaccination in the United States
df = df.filter(df.country == "United States") 
df = df.filter(df.people_vaccinated != 0) 
df = df[['date','people_vaccinated']]


#to work on the features, spark MLlib expects every value to be in numeric form
#feature 'date' is string datatype
#using StringIndexer, string type will be typecast to numeric datatype and a new dataframe is created
#new dataframe contains a new feature 'date_num' and can be used further
indexer=StringIndexer(inputCol='date', outputCol='date_num')
indexed=indexer.fit(df).transform(df)

#creating vectors from features (input => date)
assembler = VectorAssembler(inputCols=['date_num'], outputCol='features')

final_data = assembler.transform(indexed)
final_data = final_data.select("features","people_vaccinated")

#split dataset into train data and test data
train_data,test_data=final_data.randomSplit([0.7,0.3])

#specifying label (output => #people vaccinated)
lr = LinearRegression(labelCol='people_vaccinated')
lrModel = lr.fit(train_data)

#find R-squared
results=lrModel.evaluate(train_data)
print('Rsquared Error :',results.r2)

modelsummary = lrModel.summary
print(f'Intercept: {lrModel.intercept}\nCoefficient: {lrModel.coefficients.values}')


modelsummary.predictions.show(100)





