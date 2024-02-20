from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[3]").appName("Spark Scrham Demo") \
        .getOrCreate()
    schema = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    flightSchemaDDL = """ FL_DATE date,OP_CARRIER string, OP_CARRIER_FL_NUM int, ORIGIN string, ORIGIN_CITY_NAME string,
    DEST string, DEST_CITY_NAME string, CRS_DEP_TIME int, DEP_TIME int, WHEELS_ON int, TAXI_IN int, CRS_ARR_TIME int,
    ARR_TIME int, CANCELLED int, DISTANCE int"""
    flightTimeCsvDF = spark.read \
        .format('csv') \
        .option('header', "true") \
        .option("mode", "PERMISSIVE")\
        .schema(schema)\
        .load("data/flight-time.csv")

    flightTimeCsvDF.show()
    print("schema", flightTimeCsvDF.schema.simpleString())

    flightTimeJsonDF = spark.read \
        .format('json') \
        .schema(flightSchemaDDL) \
        .load("data/flight-time.json")
    flightTimeJsonDF.show()
    print("json schema:", flightTimeJsonDF.schema.simpleString())

