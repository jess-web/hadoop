from pyspark.sql import SparkSession
import pyspark
#joining two dataframe
spark=SparkSession.builder.appName("Example 1").getOrCreate()
data1=[("Name",1),("Name2",2),("Name3",3)]
data2=[("Name1","Engineer"),("Name2","Doctor"),("Name4","Student")]
col1=["Name","RollNo"]
col2=["Name","Profession"]
df1=spark.createDataFrame(data1,col1)
df2=spark.createDataFrame(data2,col2)
join_df=df1.join(df2,"Name","inner")
print("Inner Function")
join_df2=df1.join(df2,"Name","outer")
join_df.show()
print("Outer Function")
join_df2.show()
# spark.stop()
