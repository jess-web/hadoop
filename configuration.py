from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


#create a spark configuration
spark_conf=SparkConf()
spark_conf.set("spark.app.name","DataFrame")
spark_conf.set("spark.executor.memory","2g")
spark_conf.set("spark.cores.max","2")


#create a spark session using the configuration
spark=SparkSession.builder.config(conf=spark_conf).getOrCreate()


#sample data
data=[("Name1",21),("Name2",21),("Name3",22),("Name4",20),("Name5",23)]


columns="Name","Age"


#Create a dataframe from the sample data
df=spark.createDataFrame(data,columns)
df.show()


# spark.stop()
