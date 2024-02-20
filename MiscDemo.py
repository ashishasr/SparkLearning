from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("MissDemo") \
    .master("local[*]") \
    .getOrCreate()

list_data = [("Ravi", "28", "1", "2022"),
             ("Abdul", "23", "5", "81"),
             ("John", "12", "12", "6"),
             ("Rosy", "7", "8", "63"),
             ("Abdul", "23", "5", "81")
             ]
raw_df = spark.createDataFrame(list_data).toDF("name", "day", "month", "year").repartition(3)
raw_df.printSchema()
raw_df.show()

df1 = raw_df.withColumn("id", monotonically_increasing_id())

df1.show()

df2 = raw_df.withColumn("year", expr("""  case when year < 21 then cast(year as int) + 2000 
         when year < 100 then cast(year as int) + 1900 ELSE year END"""))
df2.show()

df3 = raw_df.withColumn("year", expr("""  case when year < 21 then year + 2000
         when year < 100 then year + 1900 ELSE year END""").cast(IntegerType()))
df3.show()

df5 = df1.withColumn("day", col("day").cast(IntegerType())) \
    .withColumn("month", col("month").cast(IntegerType())) \
    .withColumn("year", col("year").cast(IntegerType()))
df5.printSchema()
df5.show()

df6 = df5.withColumn("year", \
                     when(col("year") < 21, col("year") + 2000) \
                     .when(col("year") < 100, col("year") + 1900) \
                     .otherwise(col("year")))
df6.show()


