from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("WindowingDemo") \
        .master("local[2]") \
        .getOrCreate()

    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [("01", "Scroll MOuse", 250, 20),
                    ("02", "Optical MOuse", 350, 20),
                    ("03", "Wireless MOuse", 450, 50),
                    ("04", "Wireless keyboard", 580, 50),
                    ("05", "Standard keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")
    # order_df.show()
    # product_df.show()
    product_renamed_df = product_df.withColumnRenamed("qty", "reorder_qty")
    join_expr = order_df.prod_id == product_df.prod_id
    joinType = "inner"
    order_df.join(product_renamed_df, join_expr, joinType) \
            .drop(product_renamed_df.prod_id) \
            .select("order_id", "prod_id", "prod_name", "unit_price", "qty") \
            .show()

    order_df.join(product_renamed_df, join_expr, "left") \
        .drop(product_renamed_df.prod_id) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .withColumn("prod_name", expr("coalesce(prod_name, prod_id)")) \
        .withColumn("list_price", expr("coalesce(list_price, unit_price)")) \
        .sort("order_id")\
        .show()