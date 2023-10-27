from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("KMeansIris").getOrCreate()

# Load the Iris dataset
iris_data = spark.read.csv("iris.csv", header=True, inferSchema=True)

# Rename columns with dots in their names
iris_data = iris_data.withColumnRenamed("sepal.length", "sepal_length") \
                   .withColumnRenamed("sepal.width", "sepal_width") \
                   .withColumnRenamed("petal.length", "petal_length") \
                   .withColumnRenamed("petal.width", "petal_width")

# Select relevant features (attributes) for clustering
feature_columns = ["sepal_length", "sepal_width", "petal_length", "petal_width"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(iris_data)

# Train a K-Means clustering model
kmeans = KMeans().setK(3).setSeed(1)  # Set the number of clusters (K) to 3
model = kmeans.fit(data)

# Get cluster centers
cluster_centers = model.clusterCenters()
print("Cluster Centers:")
for center in cluster_centers:
    print(center)

# Assign data points to clusters
predictions = model.transform(data)

# Visualize the clustering results

# Convert the PySpark DataFrame to a Pandas DataFrame for visualization
df = predictions.select("features", "prediction").toPandas()

# Plot the clustered data points
plt.scatter(df["features"].apply(lambda x: x[0]), df["features"].apply(lambda x: x[1]), c=df["prediction"], cmap="rainbow")
plt.xlabel("Sepal Length (cm)")
plt.ylabel("Sepal Width (cm)")
plt.title("K-Means Clustering of Iris Dataset")
plt.show()

# Stop the Spark session
# spark.stop()
