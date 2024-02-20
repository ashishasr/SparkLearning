import sys

from pyspark.sql import *
from pyspark import SparkConf
from lib.logger import Log4j
from lib.utils import get_spark_app_config, load_survey_df

if __name__ == '__main__':

    conf = get_spark_app_config()
    spark = SparkSession.builder\
                        .config(conf=conf)\
                        .getOrCreate()

    logger = Log4j(spark)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)
    logger.info("Starting Hello Spark")
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    survey_df = load_survey_df(spark, sys.argv[1])
    survey_df.show()

    filtered_df = survey_df.where("Age < 40")\
                           .select("Age", "Gender", "Country", "State")
    filtered_df.show()
    grouped_df = filtered_df.groupBy("Country")
    count_df = grouped_df.count()
    count_df.show()
    logger.info("Finished Hello Spark")


