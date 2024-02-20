from pyspark.sql import SparkSession
from pyspark.sql.functions import col


class FinancialValidator:
    def __init__(self, spark):
        self.spark = spark

    def calculate_sums(self, df1, df2):
        # Calculate sums for fin_amt1, fin_amt2, and fin_amt3 in df1
        sums_df1 = df1.select([col(c).alias(f"sum{c}") for c in ["fin_amt1", "fin_amt2", "fin_amt3"]])

        # Calculate sums for fin_amt4, fin_amt5, and fin_amt6 in df2
        sums_df2 = df2.select([col(c).alias(f"sum{c}") for c in ["fin_amt4", "fin_amt5", "fin_amt6"]])

        return sums_df1, sums_df2

    def validate_conditions(self, sums_df1, sums_df2):
        # Validate conditions
        condition1 = sums_df1.select("sum_fin_amt2").first()[0] == sums_df2.select("sum_fin_amt3").first()[0]
        condition2 = sums_df1.select("sum_fin_amt3").first()[0] == sums_df2.select("sum_fin_amt4").first()[0]
        condition3 = sums_df1.select("sum_fin_amt3").first()[0] == sums_df2.select("sum_fin_amt5").first()[0]

        return condition1, condition2, condition3

    def write_to_csv(self, df1, df2, output_path):
        # Write df1 and df2 to CSV if all conditions are met
        if all(self.validate_conditions(df1, df2)):
            df1.write.csv(output_path + "/df1.csv", header=True, mode="overwrite")
            df2.write.csv(output_path + "/df2.csv", header=True, mode="overwrite")


if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder.appName("FinancialBalancing").getOrCreate()

    # Sample DataFrames df1 and df2
    data1 = [("Account1", 100.0, 50.0, 10.0), ("Account2", 200.0, 75.0, 20.0), ("Account3", 300.0, 90.0, 30.0)]
    data2 = [("Account1", 120.0, 45.0, 30.0), ("Account2", 180.0, 80.0, 25.0), ("Account3", 310.0, 95.0, 15.0)]
    columns = ["Account", "fin_amt1", "fin_amt2", "fin_amt3"]

    df1 = spark.createDataFrame(data1, columns)
    df2 = spark.createDataFrame(data2, columns)
    df1.show()
    df2.show()
    # Initialize the FinancialValidator
    validator = FinancialValidator(spark)

    # Calculate sums
    sums_df1, sums_df2 = validator.calculate_sums(df1, df2)

    # Validate conditions and write to CSV
    output_path = "/data/amex/output"
    validator.write_to_csv(sums_df1, sums_df2, output_path)

    # Stop the Spark session
    spark.stop()
