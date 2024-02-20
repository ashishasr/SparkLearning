from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, broadcast

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("BroadcastJoinDemo") \
        .master("local[2]") \
        .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 3)
flight_time_d1 = spark.read\
    .format("json")\
    .load("data/d1/")
flight_time_d2 = spark.read\
    .format("json")\
    .load("data/d2/")

# print("No of partitions for flight_time_d1=", flight_time_d1.rdd.getNumPartitions())
# print("No of partitions for flight_time_d2=", flight_time_d1.rdd.getNumPartitions())
# flight_time_d1.show()
# flight_time_d2.show()

# flight_time_renamed_df = flight_time_d2.withColumnRenamed("id", "crs_id")
join_expr = flight_time_d1.id == flight_time_d2.id
join_Type = "inner"

join_df = flight_time_d1.join(broadcast(flight_time_d2), join_expr, join_Type)
# join_df.show()

join_df.foreach(lambda f: None)
input("press key to stop......")
