from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, broadcast

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("BucketJoinDemo") \
        .master("local[2]") \
        .enableHiveSupport()\
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 3)
d1 = spark.read.json("data/d1/")
d2 = spark.read.json("data/d2/")
# flight_time_d1.show()
# flight_time_d2.show()

# spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
# spark.sql("USE MY_DB")
# flight_time_d1.coalesce(1).write\
#     .bucketBy(3, "id")\
#     .mode('overwrite')\
#     .saveAsTable("MY_DB.flight_data1")
#
# flight_time_d2.coalesce(1).write\
#     .bucketBy(3, "id")\
#     .mode("overwrite")\
#     .saveAsTable("MY_DB.flight_data2")

# Another application using the bucketed tables

df3 = spark.read.table("MY_DB.flight_data1")
df4 = spark.read.table("MY_DB.flight_data2")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

join_expr = df3.id == df4.id

join_df = df3.join(df4, join_expr, "inner")

# join_df.show()
join_df.collect()
input("Enter a key to stop.....")
