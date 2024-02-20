from pyspark.sql import functions as f
from pyspark.sql import SparkSession, Window

spark = SparkSession \
    .builder \
    .appName("WindowingDemo") \
    .master("local[2]") \
    .getOrCreate()

summary_df = spark.read \
    .format("parquet") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load("data/summary.parquet")
summary_df.show()

running_total_window = Window.partitionBy("Country") \
    .orderBy("WeekNumber") \
    .rowsBetween(-2, Window.currentRow)

running_total_df = summary_df.withColumn("running_total",
                                         f.sum("InvoiceValue").over(running_total_window))
running_total_df.show()
