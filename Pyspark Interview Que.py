# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.How to split comma-separated values into multiple rows using PySpark?
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# Step 1: Create Spark session
spark = SparkSession.builder.appName("Explode Hobbies").getOrCreate()

# Step 2: Sample data
data = [
    ("A", "Badmintion,Cricket"),
    ("B", "Cricket,Tennis"),
    ("C", "Cricket,Carrom")
]

# Step 3: Create DataFrame
df = spark.createDataFrame(data, ["Name", "Hobbies"])

# Step 4: Split and explode
df1 = df.withColumn("HobbiesArray", split(df["Hobbies"], ","))
df2 = df1.withColumn("Hobby", explode("HobbiesArray"))

# Step 5: Select final columns
result_df = df2.select("Name", "Hobby")
result_df.show(truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC ##2. FIND RECORDS WITH CORRECT MOBILE NUMBER.
# MAGIC

# COMMAND ----------


from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_replace, length

data = [
    (1, "Aman", "U43755693y"),
    (2, "Rahul", "9867456321"),
    (3, "Saurav", "M87659789U")
]

df = spark.createDataFrame(data, ["Id", "student_name", "mobile_no"])

# Step 1: Filter rows where mobile_no contains only digits
df_valid = df.filter(col("Mobile_no").rlike("^[0-9]{10}$"))

# Step 2: Sort by student name
df_sorted = df_valid.orderBy("Student_name")

# Step 3: Show result
df_sorted.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##3.Count the non null values in each columns

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count

spark= SparkSession.builder.appName("CountNonNulls").getOrCreate()

data=[
    (1, 'a', 23),
    (None, 'b', None),
    (None,'c',634),
    (None, None, None),
    (None,None,None)
]

df=spark.createDataFrame(data,["id","name","age"])

#count non-null values column-wise
non_null_counts=df.select([
    count(col("id")).alias("id_non_null"),
    count(col("name")).alias("name_non_null"),
    count(col("age")).alias("age_non_null")
])

non_null_counts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##4.Split the phone_num columns into two parts : a code and number 

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col

spark=SparkSession.builder.appName("SplitPhoneNumber").getOrCreate()

data=[
    ("A", "010-83843949"),
    ("B", "030-83843384"),
    ("C", "040-94384877")
]

df=spark.createDataFrame(data,["name","phone_num"])
df_split=df.withColumn("code",split(col("phone_num"), "-")[0]) \
    .withColumn("phone_num",split(col("phone_num"),"-")[1])

df_split.show(truncate=False)

# COMMAND ----------

