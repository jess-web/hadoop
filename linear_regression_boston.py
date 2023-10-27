from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

# Create a Spark session
spark = SparkSession.builder.appName('Regression').getOrCreate()

# Read the CSV file into a DataFrame
data1 = spark.read.csv('BostonHousing.csv', inferSchema=True, header=True)
data1.show()

# Define a VectorAssembler to combine the input columns into a 'features' column
assembler = VectorAssembler(inputCols=['crim', 'zn', 'indus', 'chas', 'nox', 'rm', 'age', 'dis', 'rad', 'tax', 'ptratio', 'b', 'lstat'],
                           outputCol="features")

data1 = assembler.transform(data1)

# Select the columns you want to use for training
final_data = data1.select("features", "medv")

# Split the data into training and testing sets
train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)

# Create a LinearRegression model
lr = LinearRegression(featuresCol="features", labelCol="medv", predictionCol="prediction_medv")

# Fit the model to the training data
lr_model = lr.fit(train_data)

# Make predictions on the test data
prediction = lr_model.transform(test_data)

# Evaluate the model using RMSE
evaluator = RegressionEvaluator(labelCol="medv", predictionCol="prediction_medv", metricName="rmse")
rmse = evaluator.evaluate(prediction)
print(rmse)
