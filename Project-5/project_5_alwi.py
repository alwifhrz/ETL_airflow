from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, date_format, count

# Create Spark session
spark = SparkSession.builder.appName("Project_5_alwi").getOrCreate()

# Read file tripdata
trips_data = spark.read.parquet("/home/dev/airflow/spark-code/Project-5/fhv_tripdata_2021-02.parquet")

# 1. Taxi trips on February 15
filtered_trips = trips_data.filter(col("pickup_datetime").startswith("2021-02-15")).count()
print(f"\nNumber of taxi trips on February 15 = {filtered_trips}\n")

# 2. The longest trip for each day
trips_data = trips_data.withColumn("pickup_date", date_format("pickup_datetime", "yyyy-MM-dd"))
longestTrips = trips_data.groupBy("pickup_date").agg(max("trip_distance").alias("max_trip_distance"))
longestTrips.show()

# 3. The Top 5 Most frequent dispatching_base_num
dispatchBaseNum = trips_data.groupBy("dispatching_base_num").agg(count("*").alias("count"))
mostFrequent = dispatchBaseNum.orderBy(col("count").desc())
mostFrequent.show(5)

# 4. The Top 5 Most common location pairs (PUlocationID and DOlocationID)
locations = trips_data.filter(col("PUlocationID").isNotNull() & col("DOlocationID").isNotNull())
top5 = locations.groupBy("PUlocationID", "DOlocationID").count().orderBy(col("count").desc())
top5.show(5)
