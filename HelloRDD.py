import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from collections import namedtuple
from lib.logger import Log4j


SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])
if __name__ == "__main__":
    conf = SparkConf()
    conf.setMaster("local[*]") \
        .setAppName("Spark RDD APP")
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()
    sc = spark.sparkContext
    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: Hello Spark")
        sys.exit(-1)
    print("file: ", str(sys.argv[1]))
    linesRdd = sc.textFile(sys.argv[1])
    logger.info("Before Repartition: " + str(linesRdd.getNumPartitions()))
    partitionedRdd = linesRdd.repartition(3)
    logger.info("After Repartition: " + str(partitionedRdd.getNumPartitions()))

    colsRDD = partitionedRdd.map(lambda line: line.replace('"', '').split(","))
    surveyRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = surveyRDD.filter(lambda r: r.Age < 40)

    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))

    countByCountryRDD = kvRDD.reduceByKey(lambda v1, v2: v1+v2)
    colsList = countByCountryRDD.collect()
    for x in colsList:
        logger.info(x)

