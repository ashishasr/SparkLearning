import sys

from pyspark.sql import SparkSession
from pyspark import SparkConf

from lib.logger import Log4j

if __name__ == '__main__':

    conf = SparkConf()
    conf.setMaster("local[*]")\
        .setAppName("Spark SQL APP")

    spark = SparkSession\
        .builder\
        .config(conf=conf)\
        .getOrCreate()

    logger = Log4j(spark)
    if len(sys.argv) != 2:
        logger.error("Usage: Hello Spark")
        sys.exit(-1)
    print("file: ", str(sys.argv[1]))

    surveyDF = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(sys.argv[1])
    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql(""" select Country, count(1) from survey_tbl where Age < 40 group by Country""")
    countDF.show()

