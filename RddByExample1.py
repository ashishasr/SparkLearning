from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
dept = [(1, 123, "HR", "Rob", 10000), \
        (2, 124, "HR", "Rom", 12000), \
        (5, 126, "Tech", "Tom", 10000), \
        (6, 127, "Tech", "Tom", 30500), \
        (7, 129, "Tech", "Tom", 40000),
        ]
schema = ["emp_no", "emp_id", "dept", "name", "salary"]

df = spark.createDataFrame(data=dept, schema=schema)
df.show()

df.createOrReplaceTempView("sales")

df1 = spark.sql(
    'select dept, sum(salary)/sum(cntr) avg from (select dept, salary, 1 as cntr from sales) x group by dept')
df1.show()

df2 = spark.sql("select dept, avg(salary) from sales group by dept")
df2.show()

df3 = spark.sql("select s1.name, s1.dept, s1.salary from sales s1 inner join  (select dept, avg(salary) avg_salary"
                " from sales group by dept) s2 ON s1.dept=s2.dept where s1.salary > s2.avg_salary ")

df3.show()

df4 = spark.sql("select s1.name, s1.dept, s1.salary from sales s1 where s1.salary > (select avg(salary) from "
                "sales s2 where s2.dept = s1.dept)")
df4.show()

df5 = spark.sql("select s1.name, s1.dept, s1.salary from sales s1 where s1.salary > (select avg(salary) from "
                "sales group by dept)")

df5.show()
