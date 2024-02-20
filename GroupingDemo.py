from pyspark.sql import functions as f
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("GroupingDemo")\
    .master("local[2]") \
    .getOrCreate()

invoices_df = spark.read \
    .format("csv") \
    .option("inferSchema", "true")\
    .option("header", "true")\
    .load("data/invoices.csv")
invoices_df.show()

NumInvoices = f.countDistinct(f.col("InvoiceNo")).alias("NumInvoices")
TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
InvoiceValue = f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")
# using object expression
ex_summary_df = invoices_df.withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), 'dd-MM-yyyy H.mm')) \
           .where("year(InvoiceDate) == 2010") \
           .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate")))\
           .groupBy("Country", "WeekNumber")\
           .agg(NumInvoices, TotalQuantity, InvoiceValue)
ex_summary_df.coalesce(1).write \
            .format("parquet")\
            .mode("overwrite")\
            .save("output")
ex_summary_df.sort("Country", "WeekNumber").show()
