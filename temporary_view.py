from pyspark.sql import SparkSession
from pyspark.sql.functions import col


#create a spark session
spark=SparkSession.builder.appName("Example1").getOrCreate()


#read the ccsv file
df=spark.read.csv("emp.csv",header=True, inferSchema=True)
df.printSchema()


#create temporary view of dataframe to use sql query
df.createOrReplaceTempView("employee") #temp table -> employee
result=spark.sql("select * from employee where age > 25 or gender='Female'")
result1=spark.sql("select * from employee where age > 25 and gender='Female'")
result.show()
result1.show()
# spark.stop()
