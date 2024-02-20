import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


def parse_gender(gender):
    male_pattern = r"^m$|ma|m.l"
    female_pattern = r"^f$|f.m|w.m"
    if re.search(male_pattern, gender):
        return 'Male'
    elif re.search(female_pattern, gender):
        return 'Female'
    else:
        return 'Unknown'


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('UDF Demo app') \
        .master("local[2]") \
        .getOrCreate()

    survey_df = spark.read \
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv("data/survey.csv")
    # survey_df.printSchema()

    survey_df.show()

    # register custom function using udf() to spark session (driver), Register as  a dataframe column object expression
    parsed_gender_udf = udf(parse_gender, StringType())
    print("Catalogue Entry:")
    [print(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    # 1. Use Column Object Expression
    survey_df2 = survey_df.withColumn("Gender", parsed_gender_udf("Gender"))

    survey_df2.show()

    # 2. Use column String Expression, Register it as SQL function and create an entry in catalogue
    spark.udf.register("parsed_gender_udf", parse_gender, StringType())
    print("Catalogue Entry:")
    [print(f.name) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df3 = survey_df.withColumn("Gender", expr("parsed_gender_udf(Gender)"))
    survey_df3.show()
