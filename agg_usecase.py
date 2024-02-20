from pyspark.sql import SparkSession
from pyspark.sql import functions as f

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Invoice Data Agg Usecase Demo") \
        .master("local[2]") \
        .getOrCreate()

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("data/invoices.csv")

    invoice_df.select(f.count("*").alias("Count *"),
                      f.sum("Quantity").alias("TotalQuantity"),
                      f.avg("UnitPrice").alias("AvgPrice"),
                      f.countDistinct("InvoiceNo").alias("CountDistinct")
                      ).show()

    invoice_df.selectExpr(
        "count(1) as `count 1`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as TotalQuantity",
        "avg(UnitPrice) as AvgPrice"
    ).show()

    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
          SELECT Country, InvoiceNo,
                sum(Quantity) as TotalQuantity,
                round(sum(Quantity*UnitPrice),2) as InvoiceValue
          FROM sales
          GROUP BY Country, InvoiceNo""")

    summary_sql.show()

    summary_df = invoice_df \
        .groupBy("Country", "InvoiceNo") \
        .agg(f.sum("Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValueExpr")
             )
    summary_df.show()
    summary_df.write.json("data/agg_output.json")
    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS invoice")

    # Create Hive Internal table
    summary_df.write.format("json").mode('overwrite') \
        .saveAsTable("invoice.invoice_agg")

    # Spark read Hive table
    df = spark.read.table("invoice.invoice_agg")
    df.show()
