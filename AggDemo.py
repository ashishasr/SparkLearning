from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("AggDemo") \
    .master("local[*]") \
    .getOrCreate()

invoices_df = spark.read \
              .format("csv") \
              .option("inferSchema", "true") \
              .option("header", "true") \
              .load("data/invoices.csv")
invoices_df.show()

invoices_df.select(f.count("*").alias("count"),
                   f.sum("Quantity").alias("TotalQuantity"),
                   f.avg("UnitPrice").alias("AvgPrice"),
                   f.countDistinct("InvoiceNo").alias("countDistinct")).show()

invoices_df.createOrReplaceTempView("sales")
summary_sql = spark.sql("""  
SELECT Country, InvoiceNo,
       sum(Quantity)  as TotalQuantity,
       round(sum(Quantity * UnitPrice),2) as InvoiceValue
       FROM sales 
       GROUP BY Country, InvoiceNo""")

summary_sql.show()

summary_df = invoices_df \
            .groupBy("Country", "invoiceNo") \
            .agg(f.sum("Quantity").alias("TotalQuantity"), \
                 f.round(f.sum(f.expr("Quantity * UnitPrice"))).alias("InvoiceValue"),
                 f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr"))
summary_df.show()

invoices_df.printSchema()

