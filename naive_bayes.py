#Naive Bayes
#Loading the library
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from sklearn.metrics import confusion_matrix
from sklearn.datasets import load_iris
import pandas as pd


#Preparing the data
iris=load_iris()
df_iris=pd.DataFrame(iris.data,columns=iris.feature_names)
print(df_iris.head())


df_iris['label']=pd.Series(iris.target)


print(df_iris.tail())


#Define the sql content and create
sc=SparkContext.getOrCreate()
SqlContext=SQLContext(sc)
data=SqlContext.createDataFrame(df_iris)
data.printSchema()


#Combine the features of data and separate labels while using VectorAssembler
features=iris.feature_names
va=VectorAssembler(inputCols=features,outputCol='features')
va_df=va.transform(data)
va_df=va_df.select(['features','label'])
print("Combining the features into one column")
va_df.show(3)


#Split the data into training and testing
(train,test)=va_df.randomSplit([0.9,0.1])
#Prediction and accurancy

#Decision Tree Classifier by using naive bayes class and fit the model into train data
nb=NaiveBayes(smoothing=1.0,modelType='multinomial')
nb=nb.fit(train)
pred=nb.transform(test)
print("Prediction")
pred.show(7)


#predict the test data and check the accuracy matrix
evaluator=MulticlassClassificationEvaluator(predictionCol='prediction')
acc=evaluator.evaluate(pred)
print('Prediction accuracy',acc)
y_pred=pred.select("Prediction").collect()
y_orig=pred.select("label").collect()
cm=confusion_matrix(y_orig,y_pred)
print("Confusion Matrxi",cm)
# sc.stop()
