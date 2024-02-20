import configparser
import sys

from pyspark import SparkConf


def get_spark_app_config():
    conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, value) in config.items("SPARK_APP_CONFIGS"):
        conf.set(key, value)
    return conf


def load_survey_df(spark, datafile):
    return spark.read \
        .option("header", "true") \
        .option("InferSchema", "true") \
        .csv(datafile)
