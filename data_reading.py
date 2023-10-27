from pyspark.sql import SparkSession
import pyspark
#initialize the spark
spark=SparkSession.builder.appName("Example 1").getOrCreate()
data_df=spark.read.csv("Employee.csv")
data_df.show()
