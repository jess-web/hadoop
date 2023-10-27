from pyspark import SparkContext
from pyspark.conf import SparkConf
conf=SparkConf().setAppName("Word Count Example")
sc=SparkContext.getOrCreate(conf=conf)
data=["apple","orange","banana","apple","orange","banana","apple","apple"]
rdd=sc.parallelize(data)
word_counts=rdd.flatMap(lambda line:line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a,b:a+b)


print("Word Count: ",word_counts.collect())


#distinct operation
distinct_word=rdd.flatMap(lambda lines: lines.split(" ")).distinct()
print("Distinct Operation",distinct_word.collect())


#create a list of characters
char_list=rdd.flatMap(lambda line:list(line))
print("List of Characters",char_list.collect())


# sc.stop()
