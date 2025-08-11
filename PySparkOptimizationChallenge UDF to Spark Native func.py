🔥 I reduced a PySpark job time on 50M+ records… just by removing ONE thing.

🚀 #PySparkOptimizationChallenge
While working with a large dataset, I spotted performance bottlenecks caused by UDF usage and too many transformations.

❌ Inefficient Approach:


@udf(StringType())
def format_user_id(id):
    return f"user-{id}"

df = df.repartition(2)
df = df.withColumn("user_id", format_user_id(df["id"]))
df = df.withColumn("activity_score", (rand() * 100).cast("int"))
df = df.withColumn("session_time_sec", (rand() * 10000).cast("int"))
df = df.withColumn("region", lit("US-East"))
df.cache()
active_users = df.filter("activity_score > 20 AND session_time_sec > 500")
wide_df = active_users.select("user_id", "activity_score", "session_time_sec", "region")
wide_df = wide_df.distinct()
data = wide_df.collect()  # costly!


✅ Optimized Approach:
df = spark.range(0, 50000000).select(
    concat_ws("-", lit("user"), col("id")).alias("user_id"),
    (rand() * 100).cast("int").alias("activity_score"),
    (rand() * 10000).cast("int").alias("session_time_sec"),
    lit("US-East").alias("region")
).cache()

active_users = df.filter(
    (col("activity_score") > 20) & 
    (col("session_time_sec") > 500)
)

active_users.select("user_id", "activity_score", "session_time_sec", "region").show(5, truncate=False)

🔍 Key Improvements:
✅ Replaced UDF with native Spark functions → Catalyst Optimizer can work its magic
✅ Combined multiple withColumn() into one select() → fewer stages
❌ Avoided .collect() → keeps data distributed
❌ Removed unnecessary repartition(2) & distinct()

📌 Remember:
UDF = black box for Catalyst → harder to optimize
.collect() = potential driver memory overload

💡 Even small code changes can deliver huge performance gains at scale.

