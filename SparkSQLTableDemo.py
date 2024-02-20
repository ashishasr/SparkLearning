from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession\
        .builder\
        .master("local[3]")\
        .appName("Spark SQLTable Demo")\
        .enableHiveSupport()\
        .getOrCreate()

    flightTimeDF = spark.read\
        .format("parquet")\
        .load("data/flight-time.parquet")
    flightTimeDF.printSchema()
    flightTimeDF.show(10)

    spark.sql("CREATE DATABASE IF NOT EXISTS flight_db")
    spark.catalog.setCurrentDatabase("\")

    flightTimeDF.write\
                .format("csv")\
                .mode("overwrite")\
                .bucketBy(5, "OP_CARRIER", "ORIGIN")\
                .sortBy("OP_CARRIER", "ORIGIN")\
                .saveAsTable("flight_data_tbl")


