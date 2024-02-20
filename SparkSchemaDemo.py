from pyspark.sql import SparkSession


if __name__ == '__main__':

    spark = SparkSession.builder.master("local[3]").appName("Spark Scrham Demo")\
                .getOrCreate()
    flightTimeCsvDF = spark.read\
                     .format('csv')\
                     .option('header', "true")\
                     .option('InferSchema', 'true')\
                     .load("data/flight-time.csv")
    flightTimeCsvDF.show()
    print("schema", flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = spark.read \
        .format('json') \
        .load("data/flight-time.json")

    print("json schema:", flightTimeJsonDF.schema.simpleString())

    flightTimeParquetDF = spark.read \
        .format('parquet') \
        .load("data/flight-time.parquet")

    print("parquet schema:", flightTimeParquetDF.schema.simpleString())
