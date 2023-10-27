from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


#create a spark session
spark=SparkSession.builder.appName("Example1").getOrCreate()


#read the ccsv file
df=spark.read.csv("emp.csv",header=True, inferSchema=True)
df.printSchema()


#define a funtion that we need to use as udf : user define function
def greet(name):
  return f"Hello,{name}"


greet_udf=udf(greet,StringType())
df_greeting=df.withColumn("Greeting",greet_udf(df["Name"]))
df_greeting.show()
# spark.stop()
