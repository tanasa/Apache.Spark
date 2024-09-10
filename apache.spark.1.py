#!/usr/bin/env python


get_ipython().system('pip install pyspark')
import pyspark


# In[1]:


import pyspark
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler,VectorIndexer
from pyspark.ml.regression import LinearRegression


# In[2]:


from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("PySparkExample").getOrCreate()

# Create a DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Print the DataFrame
df.show()

# Perform a simple transformation
df_filtered = df.filter(df["Age"] > 30)

# Print the filtered DataFrame
df_filtered.show()

# Stop the SparkSession
# spark.stop()

# Selecting specific columns
df.select("Name", "Age").show()


# In[4]:


# Sorting the DataFrame
df_sorted = df.orderBy(df["Age"])
df_sorted.show()


# In[5]:


# Adding a new column
# df_with_gender = df.withColumn("Gender", "Female")
# df_with_gender.show()

# Grouping and aggregating data
# df_grouped = df.groupBy("Gender").agg({"Age": "avg"})
# df_grouped.show()


# In[6]:


# Reading a CSV file
# df_csv = spark.read.csv("data.csv", header=True, inferSchema=True)

# Writing a DataFrame to a CSV file
# df_csv.write.csv("output.csv", header=True)


# In[7]:


# Parquet

# Parquet is a columnar storage file format optimized for big data processing. 
# PySpark provides built-in support for reading and writing Parquet files. 

# Reading a Parquet file
# df_parquet = spark.read.parquet("data.parquet")

# Writing a DataFrame to a Parquet file
# df_parquet.write.parquet("output.parquet")


# In[8]:


# Reading a JSON file
# df_json = spark.read.json("data.json")

# Writing a DataFrame to a JSON file
# df_json.write.json("output.json")


# In[9]:


data


# In[10]:


# Defining a Schema

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema
schema = StructType([
    StructField("Name", StringType(), nullable=False),
    StructField("Age", IntegerType(), nullable=True),
])

# Create a DataFrame with the defined schema
df_with_schema = spark.createDataFrame(data, schema)
df_with_schema.show()


# In[11]:


# Renaming a column
df_renamed = df.withColumnRenamed("Name", "First Name")
df_renamed.show()


# In[12]:


# Dropping a column
df_dropped = df.drop("Age")
df_dropped.show()


# In[13]:


# Checking for null values
df_null = df.filter(df["Age"].isNull())

# Filling null values with a default value
df_filled = df.fillna({"Age": 0})

# Dropping rows with null values
df_no_null = df.dropna()
df_no_null.show()


# In[14]:


# Dropping rows with missing data
df_no_missing = df.dropna()


# In[15]:


# Filling missing data with a default value
df_filled = df.fillna(0)

# Filling missing data with the mean value of a column
mean_age = df.select("Age").agg({"Age": "mean"}).first()[0]
df_filled = df.fillna


# In[16]:


# Handling Missing Data with Imputation

from pyspark.ml.feature import Imputer

# Create an Imputer object
imputer = Imputer(inputCols=["Age"], outputCols=["Age_imputed"])

# Fit the imputer model on the DataFrame
imputer_model = imputer.fit(df)

# Apply the imputation on the DataFrame
df_imputed = imputer_model.transform(df)
df_imputed.show()


# Grouping and aggregating data

# df_grouped = df.groupBy("Gender").agg({"Age": "avg", "Salary": "sum"})
# df_grouped.show()

# Calculating descriptive statistics

# df_stats = df.describe(["Age", "Salary"])
# df_stats.show()


# Creating a pivot table

# df_pivot = df.groupBy("Gender").pivot("City").agg({"Salary": "sum"})
# df_pivot.show()


# Inner join :

# Performing an inner join
df_joined = df1.join(df2, df1["ID"] == df2["ID"], "inner")
df_joined.show()


# Outer join :

# Performing an outer join
df_joined = df1.join(df2, df1["ID"] == df2["ID"], "outer")
df_joined.show()


# Union :

# Combining DataFrames using union
df_combined = df1.union(df2)
df_combined.show()


# Streaming Data Frame :

# pip install kafka-python
import kafka-python


from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType
import kafka

# Define the schema for the streaming data
schema = StructType().add("name", StringType()).add("age", IntegerType())

# Create a Streaming DataFrame from a Kafka topic
streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic-name") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))


# Write the transformed streaming data to the console
query = streaming_df.writeStream.outputMode("append").format("console").start()

# To keep the streaming job running until explicitly stopped.
query.awaitTermination()