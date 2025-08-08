**PySpark Optimization Challenge**

**#ProductBasedInterview** 



*While working with large datasets in PySpark, I noticed performance bottlenecks due to UDF usage and multiple transformations.*

*How will you optimize it?*



❌Inefficient Approach



@udf(StringType())

def format\_user\_id(id):

&nbsp;   return f"user-{id}"



df = df.repartition(2)

&nbsp;



df = df.withColumn("user\_id", format\_user\_id(df\["id"]))



df = df.withColumn("activity\_score", (rand() \* 100).cast("int"))

df = df.withColumn("session\_time\_sec", (rand() \* 10000).cast("int"))

df = df.withColumn("region", lit("US-East"))



df.cache()



active\_users = df.filter("activity\_score > 20 AND session\_time\_sec > 500")



wide\_df = active\_users.select("user\_id", "activity\_score", "session\_time\_sec", "region")

&nbsp;



wide\_df = wide\_df.distinct()



data = wide\_df.collect()



print(f"Collected {len(data)} active user records to the driver.")





While optimizing a PySpark job with 50M records, I applied several key transformations for better performance and scalability.



✅ Optimized Approach



df = spark.range(0, 50000000)



df = df.select(

&nbsp;   concat\_ws("-", lit("user"), col("id")).alias("user\_id"),

&nbsp;   (rand() \* 100).cast("int").alias("activity\_score"),

&nbsp;   (rand() \* 10000).cast("int").alias("session\_time\_sec"),

&nbsp;   lit("US-East").alias("region")

).cache()



active\_users = df.filter((col("activity\_score") > 20) \& (col("session\_time\_sec") > 500))

wide\_df = active\_users.select("user\_id", "activity\_score", "session\_time\_sec", "region")

wide\_df.show(5, truncate=False)  # avoid collect()





🔍 Key Improvements:

✅ Replaced UDF with native Spark functions



✅ Combined withColumn() alls into a single select() for better optimization



❌ Avoided .collect() — keeps data distributed across the cluster.



❌ Avoided unnecessary repartition(2) and distinct()



📌 Remember: 



\* UDF = black box for spark's catalyst optimizer  --> harder to Optimize,

\*.collect() = risk for memory overload on driver.





💡 Small changes in PySpark code can lead to huge performance gains at scale.



\#PySpark #ApacheSpark #DataEngineering #SparkOptimization #BigData #UDF #PerformanceMatters #ETL #LinkedInTech





